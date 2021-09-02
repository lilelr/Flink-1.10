/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.util;

import org.apache.flink.mesos.Utils;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.flink.mesos.Utils.UNRESERVED_ROLE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An allocation of resources on a particular host from one or more Mesos offers, to be portioned out to tasks.
 *
 * <p>A typical offer contains a mix of reserved and unreserved resources.
 * The below example depicts 2 cpus reserved for 'myrole' plus 3 unreserved cpus for a total of 5 cpus:
 * <pre>{@code
 *   cpus(myrole):2.0; mem(myrole):4096.0; ports(myrole):[1025-2180];
 *   disk(*):28829.0; cpus(*):3.0; mem(*):10766.0; ports(*):[2182-3887,8082-8180,8182-32000]
 * }</pre>
 *
 * <p>This class assumes that the resources were offered <b>without</b> the {@code RESERVATION_REFINEMENT} capability,
 * as detailed in the "Resource Format" section of the Mesos protocol definition.
 *
 * <p>This class is not thread-safe.
 */
public class MesosResourceAllocation {

	protected static final Logger LOG = LoggerFactory.getLogger(MesosResourceAllocation.class);

	static final double EPSILON = 1e-5;

	private final List<Protos.Resource> resources;

	private Map<String, Protos.Attribute> attributeMap;

	/**
	 * Creates an allocation of resources for tasks to take.
	 *
	 * @param resources the resources to add to the allocation.
	 */
	public MesosResourceAllocation(Collection<Protos.Resource> resources) {
		this.resources = new ArrayList<>(checkNotNull(resources));
//		LOG.info(resources.iterator().next().);
		// sort the resources to prefer reserved resources
		this.resources.sort(Comparator.comparing(r -> UNRESERVED_ROLE.equals(r.getRole())));
	}

	public MesosResourceAllocation(Collection<Protos.Resource> resources, Map<String, Protos.Attribute> attributes ) {
		this.resources = new ArrayList<>(checkNotNull(resources));
//		LOG.info(resources.iterator().next().);
		// sort the resources to prefer reserved resources
		this.resources.sort(Comparator.comparing(r -> UNRESERVED_ROLE.equals(r.getRole())));

		this.attributeMap = attributes;
	}

	/**
	 * Gets the remaining resources.
	 */
	public List<Protos.Resource> getRemaining() {
		return Collections.unmodifiableList(resources);
	}

	/**
	 * Takes some amount of scalar resources (e.g. cpus, mem).
	 *
	 * @param amount the (approximate) amount to take from the available quantity.
	 * @param roles the roles to accept
	 */
	public List<Protos.Resource> takeScalar(String resourceName, double amount, Set<String> roles) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Allocating {} {}", amount, resourceName);
		}
		LOG.info("lele allocating resources for tasks begins, takeScalar");
		if(this.attributeMap.containsKey("lele_mem")){
			double mem_scale = this.attributeMap.get("lele_mem").getScalar().getValue();
			LOG.info("lele mem_scale "+mem_scale);
			if(resourceName.equals("mem")){
				LOG.info("lele scale up memory for this task before"+amount);
				amount = amount+mem_scale;
				LOG.info("lele scale up memory for this task after"+amount);
			}
		}

		if(this.attributeMap.containsKey("lele_cpus")){
			double cpu_scale = this.attributeMap.get("lele_cpus").getScalar().getValue();
			LOG.info("lele cpu_scale "+cpu_scale);
			if(resourceName.equals("cpus")){
				LOG.info("lele scale up cpus for this task before"+amount);
				amount = amount+cpu_scale;
				LOG.info("lele scale up cpus for this task after"+amount);
			}
		}
		List<Protos.Resource> result = new ArrayList<>(1);
		for (ListIterator<Protos.Resource> i = resources.listIterator(); i.hasNext();) {
			if (amount <= EPSILON) {
				break;
			}

			// take from next available scalar resource that is unreserved or reserved for an applicable role
			Protos.Resource available = i.next();
			LOG.info("lele resourceName is "+resourceName);
			LOG.info("lele available.getName() " + available.getName());
			if (!resourceName.equals(available.getName()) || !available.hasScalar()) {
				continue;
			}
			if (!UNRESERVED_ROLE.equals(available.getRole()) && !roles.contains(available.getRole())) {
				continue;
			}

			if(available.hasReservation()){
				Protos.Resource.ReservationInfo reservationInfo = available.getReservation();
				Protos.Labels labels = reservationInfo.getLabels();
				List<Protos.Label> labelList = labels.getLabelsList();
				for(Protos.Label label:labelList){
					if(label.getKey().equals("cpus") && resourceName.equals("cpus")){
						LOG.info("lele scale up 2 cpus "+resourceName);
						amount = amount + 2;
					}
					if(label.getKey().equals("mem") && resourceName.equals("mem")){
						LOG.info("lele scale up 500 mem "+resourceName);
						amount = amount + 500;
					}
				}
			}

			double amountToTake = Math.min(available.getScalar().getValue(), amount);
			Protos.Resource taken = available.toBuilder().setScalar(Protos.Value.Scalar.newBuilder().setValue(amountToTake)).build();
			amount -= amountToTake;
			result.add(taken);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Taking {} from {}", amountToTake, Utils.toString(available));
			}
			LOG.info("lele Taking {} from {}", amountToTake, Utils.toString(available));

			// keep remaining amount (if any)
			double remaining = available.getScalar().getValue() - taken.getScalar().getValue();
			if (remaining > EPSILON) {
				i.set(available.toBuilder().setScalar(Protos.Value.Scalar.newBuilder().setValue(remaining)).build());
			}
			else {
				i.remove();
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Allocated: {}, unsatisfied: {}", Utils.toString(result), amount);
		}
		return result;
	}

	/**
	 * Takes some amount of range resources (e.g. ports).
	 *
	 * @param amount the number of values to take from the available range(s).
	 * @param roles the roles to accept
	 */
	public List<Protos.Resource> takeRanges(String resourceName, int amount, Set<String> roles) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Allocating {} {}", amount, resourceName);
		}

		List<Protos.Resource> result = new ArrayList<>(1);
		for (ListIterator<Protos.Resource> i = resources.listIterator(); i.hasNext();) {
			if (amount <= 0) {
				break;
			}

			// take from next available range resource that is unreserved or reserved for an applicable role
			Protos.Resource available = i.next();
			if (!resourceName.equals(available.getName()) || !available.hasRanges()) {
				continue;
			}
			if (!UNRESERVED_ROLE.equals(available.getRole()) && !roles.contains(available.getRole())) {
				continue;
			}

			List<Protos.Value.Range> takenRanges = new ArrayList<>();
			List<Protos.Value.Range> remainingRanges = new ArrayList<>(available.getRanges().getRangeList());
			for (ListIterator<Protos.Value.Range> j = remainingRanges.listIterator(); j.hasNext();) {
				if (amount <= 0) {
					break;
				}

				// take from next available range (note: ranges are inclusive)
				Protos.Value.Range availableRange = j.next();
				long amountToTake = Math.min(availableRange.getEnd() - availableRange.getBegin() + 1, amount);
				Protos.Value.Range takenRange = availableRange.toBuilder().setEnd(availableRange.getBegin() + amountToTake - 1).build();
				amount -= amountToTake;
				takenRanges.add(takenRange);

				// keep remaining range (if any)
				long remaining = availableRange.getEnd() - takenRange.getEnd();
				if (remaining > 0) {
					j.set(availableRange.toBuilder().setBegin(takenRange.getEnd() + 1).build());
				}
				else {
					j.remove();
				}
			}
			Protos.Resource taken = available.toBuilder().setRanges(Protos.Value.Ranges.newBuilder().addAllRange(takenRanges)).build();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Taking {} from {}", Utils.toString(taken.getRanges()), Utils.toString(available));
			}
			result.add(taken);

			// keep remaining ranges (if any)
			if (remainingRanges.size() > 0) {
				i.set(available.toBuilder().setRanges(Protos.Value.Ranges.newBuilder().addAllRange(remainingRanges)).build());
			}
			else {
				i.remove();
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Allocated: {}, unsatisfied: {}", Utils.toString(result), amount);
		}
		return result;
	}

	@Override
	public String toString() {
		return "MesosResourceAllocation{" +
			"resources=" + Utils.toString(resources) +
			'}';
	}
}
