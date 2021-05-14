// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package org.apache.flink.api.java.tuple.builder;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple0;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Tuple0}.
 */
@Public
public class Tuple0Builder {

	private List<Tuple0> tuples = new ArrayList<Tuple0>();

	public Tuple0Builder add() {
		tuples.add(Tuple0.INSTANCE);
		return this;
	}

	public Tuple0[] build() {
		return tuples.toArray(new Tuple0[tuples.size()]);
	}

}
