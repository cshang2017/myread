package org.apache.flink.runtime.akka;

import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategyConfigurator;
import akka.japi.pf.PFBuilder;

/**
 * Escalating supervisor strategy.
 */
public class EscalatingSupervisorStrategy implements SupervisorStrategyConfigurator {

	@Override
	public SupervisorStrategy create() {
		return new OneForOneStrategy(
			false,
			new PFBuilder<Throwable, SupervisorStrategy.Directive>()
				.matchAny(
					(ignored) -> SupervisorStrategy.escalate())
				.build());
	}
}
