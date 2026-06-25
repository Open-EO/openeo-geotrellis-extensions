package org.openeo.geotrelliscommon;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method on a datacube (or its implementations) as an
 * openEO process. The Python wrapper auto-discovers these via
 * process registry at runtime.
 *
 * <p>Adding this annotation to a new Scala method is sufficient to make the
 * process available to the Python side — no Python code changes are needed
 * unless Python-specific logic (UDF handling, metadata updates) is required.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OpenEOProcess {
    /** openEO process id (e.g. "apply", "filter_temporal", "resample_spatial"). */
    String id();

    /** Human-readable description of the process. */
    String description() default "";

    /**
     * What the process returns: "datacube" (default) means the result is a
     * HealpixDatacube that should be wrapped; "other" means the raw result
     * is returned to Python.
     */
    String returns() default "datacube";
}

