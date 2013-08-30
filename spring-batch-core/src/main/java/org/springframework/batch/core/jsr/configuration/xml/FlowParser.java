/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.jsr.configuration.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.core.configuration.xml.AbstractFlowParser;
import org.springframework.batch.core.configuration.xml.SimpleFlowFactoryBean;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Parses flows as defined in JSR-352.  The current state parses a flow
 * as it is within a regular Spring Batch job/flow.
 *
 * @author Michael Minella
 * @since 3.0
 */
public class FlowParser extends AbstractFlowParser {

	private static final String DECISION_ELEMENT = "decision";
	private static final String SPLIT_ELEMENT = "split";
	private static final String STEP_ELEMENT = "step";
	private StepParser stepParser = new StepParser();
	private String flowName;

	/**
	 * @param flowName
	 * @param jobFactoryRef
	 */
	public FlowParser(String flowName, String jobFactoryRef) {
		super.setJobFactoryRef(jobFactoryRef);
		this.flowName = flowName;
	}

	@Override
	protected Class<?> getBeanClass(Element element) {
		return SimpleFlowFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		builder.getRawBeanDefinition().setAttribute("flowName", flowName);
		builder.addPropertyValue("name", flowName);

		List<BeanDefinition> stateTransitions = new ArrayList<BeanDefinition>();

		Map<String, Set<String>> reachableElementMap = new HashMap<String, Set<String>>();
		String startElement = null;
		NodeList children = element.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node node = children.item(i);
			if (node instanceof Element) {
				String nodeName = node.getLocalName();
				Element child = (Element) node;
				if (nodeName.equals(STEP_ELEMENT)) {
					stateTransitions.addAll(stepParser.parse(child, parserContext, builder));
				} else if(nodeName.equals(SPLIT_ELEMENT)) {
					stateTransitions.addAll(new SplitParser(flowName).parse(child, parserContext));
				} else if(nodeName.equals(DECISION_ELEMENT)) {
					stateTransitions.addAll(new DecisionParser().parse(child, parserContext, flowName));
				}
			}
		}

		Set<String> allReachableElements = new HashSet<String>();
		findAllReachableElements(startElement, reachableElementMap, allReachableElements);
		for (String elementId : reachableElementMap.keySet()) {
			if (!allReachableElements.contains(elementId)) {
				parserContext.getReaderContext().error("The element [" + elementId + "] is unreachable", element);
			}
		}

		ManagedList<BeanDefinition> managedList = new ManagedList<BeanDefinition>();
		managedList.addAll(stateTransitions);
		builder.addPropertyValue("stateTransitions", managedList);
	}


	public static Collection<BeanDefinition> getNextElements(ParserContext parserContext, BeanDefinition stateDef,
			Element element) {
		return getNextElements(parserContext, null, stateDef, element);
	}

	public static Collection<BeanDefinition> getNextElements(ParserContext parserContext, String stepId,
			BeanDefinition stateDef, Element element) {

		Collection<BeanDefinition> list = new ArrayList<BeanDefinition>();

		String shortNextAttribute = element.getAttribute("next");
		boolean hasNextAttribute = StringUtils.hasText(shortNextAttribute);
		if (hasNextAttribute) {
			list.add(getStateTransitionReference(parserContext, stateDef, null, shortNextAttribute));
		}

		boolean transitionElementExists = false;
		List<String> patterns = new ArrayList<String>();
		for (String transitionName : new String[] { "next", "stop", "end", "fail" }) {
			List<Element> transitionElements = DomUtils.getChildElementsByTagName(element, transitionName);
			for (Element transitionElement : transitionElements) {
				verifyUniquePattern(transitionElement, patterns, element, parserContext);
				list.addAll(parseTransitionElement(transitionElement, stepId, stateDef, parserContext));
				transitionElementExists = true;
			}
		}

		if (!transitionElementExists) {
			list.addAll(createTransition(FlowExecutionStatus.FAILED, FlowExecutionStatus.FAILED.getName(), null, null,
					stateDef, parserContext, false));
			list.addAll(createTransition(FlowExecutionStatus.UNKNOWN, FlowExecutionStatus.UNKNOWN.getName(), null, null,
					stateDef, parserContext, false));
			if (!hasNextAttribute) {
				list.addAll(createTransition(FlowExecutionStatus.COMPLETED, null, null, null, stateDef, parserContext,
						false));
			}
		}
		else if (hasNextAttribute) {
			parserContext.getReaderContext().error(
					"The <" + element.getNodeName() + "/> may not contain a 'next"
							+ "' attribute and a transition element", element);
		}

		return list;
	}

	protected static Collection<BeanDefinition> parseTransitionElement(Element transitionElement, String stateId,
			BeanDefinition stateDef, ParserContext parserContext) {

		FlowExecutionStatus status = getBatchStatusFromEndTransitionName(transitionElement.getNodeName());
		String onAttribute = transitionElement.getAttribute("on");
		String restartAttribute = transitionElement.getAttribute("restart");
		String nextAttribute = transitionElement.getAttribute("to");
		if (!StringUtils.hasText(nextAttribute)) {
			nextAttribute = restartAttribute;
		}
		boolean abandon = stateId != null && StringUtils.hasText(restartAttribute) && !restartAttribute.equals(stateId);
		String exitCodeAttribute = transitionElement.getAttribute("exit-status");

		return createTransition(status, onAttribute, nextAttribute, exitCodeAttribute, stateDef, parserContext, abandon);
	}}
