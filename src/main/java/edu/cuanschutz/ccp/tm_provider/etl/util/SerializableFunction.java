package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Allows Lambda functions to be serializable
 *
 * @param <I>
 * @param <O>
 */
@FunctionalInterface
public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {
}