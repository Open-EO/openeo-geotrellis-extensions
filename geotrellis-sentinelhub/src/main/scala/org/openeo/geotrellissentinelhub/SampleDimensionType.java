/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2011, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2005 Open Geospatial Consortium Inc.
 *    
 *    All Rights Reserved. http://www.opengis.org/legal/
 */
package org.openeo.geotrellissentinelhub;

import java.awt.image.DataBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Specifies the various dimension types for coverage values.
 * For grid coverages, these correspond to band types.
 *
 *
 *
 *
 * @source $URL$
 * @version <A HREF="http://www.opengis.org/docs/01-004.pdf">Grid Coverage specification 1.0</A>
 * @author  Martin Desruisseaux (IRD)
 * @since   GeoAPI 1.0
 *
 * @see SampleDimension
 */
public final class SampleDimensionType  {
    /**
     * Serial number for compatibility with different versions.
     */
    private static final long serialVersionUID = -4153433145134818506L;

    /**
     * List of all enumerations of this type.
     * Must be declared before any enum declaration.
     */
    private static final List<SampleDimensionType> VALUES = new ArrayList<SampleDimensionType>(11);

    /**
     * Unsigned 1 bit integers.
     *
     * @rename Renamed {@code CV_1BIT} as {@code UNSIGNED_1BIT} since we
     *         drop the prefix, but can't get a name starting with a digit.
     */
    public static final SampleDimensionType UNSIGNED_1BIT = new SampleDimensionType("UNSIGNED_1BIT");

    /**
     * Unsigned 2 bits integers.
     *
     * @rename Renamed {@code CV_2BIT} as {@code UNSIGNED_2BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     */
    public static final SampleDimensionType UNSIGNED_2BITS = new SampleDimensionType("UNSIGNED_2BITS");

    /**
     * Unsigned 4 bits integers.
     *
     * @rename Renamed {@code CV_4BIT} as {@code UNSIGNED_4BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     */
    public static final SampleDimensionType UNSIGNED_4BITS = new SampleDimensionType("UNSIGNED_4BITS");

    /**
     * Unsigned 8 bits integers.
     *
     * @rename Renamed {@code CV_8BIT_U} as {@code UNSIGNED_8BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #SIGNED_8BITS
     * @see DataBuffer#TYPE_BYTE
     */
    public static final SampleDimensionType UNSIGNED_8BITS = new SampleDimensionType("UNSIGNED_8BITS");

    /**
     * Signed 8 bits integers.
     *
     * @rename Renamed {@code CV_8BIT_S} as {@code SIGNED_8BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #UNSIGNED_8BITS
     */
    public static final SampleDimensionType SIGNED_8BITS = new SampleDimensionType("SIGNED_8BITS");

    /**
     * Unsigned 16 bits integers.
     *
     * @rename Renamed {@code CV_16BIT_U} as {@code UNSIGNED_16BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #SIGNED_16BITS
     * @see DataBuffer#TYPE_USHORT
     */
    public static final SampleDimensionType UNSIGNED_16BITS = new SampleDimensionType("UNSIGNED_16BITS");

    /**
     * Signed 16 bits integers.
     *
     * @rename Renamed {@code CV_16BIT_S} as {@code SIGNED_16BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #UNSIGNED_16BITS
     * @see DataBuffer#TYPE_SHORT
     */
    public static final SampleDimensionType SIGNED_16BITS = new SampleDimensionType("SIGNED_16BITS");

    /**
     * Unsigned 32 bits integers.
     *
     * @rename Renamed {@code CV_32BIT_U} as {@code UNSIGNED_32BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #SIGNED_32BITS
     */
    public static final SampleDimensionType UNSIGNED_32BITS = new SampleDimensionType("UNSIGNED_32BITS");

    /**
     * Signed 32 bits integers.
     *
     * @rename Renamed {@code CV_32BIT_S} as {@code SIGNED_32BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #UNSIGNED_32BITS
     * @see DataBuffer#TYPE_INT
     */
    public static final SampleDimensionType SIGNED_32BITS = new SampleDimensionType("SIGNED_32BITS");

    /**
     * Simple precision floating point numbers.
     *
     * @rename Renamed {@code CV_32BIT_REAL} as {@code REAL_32BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #REAL_64BITS
     * @see DataBuffer#TYPE_FLOAT
     */
    public static final SampleDimensionType REAL_32BITS = new SampleDimensionType("REAL_32BITS");

    /**
     * Double precision floating point numbers.
     *
     * @rename Renamed {@code CV_64BIT_REAL} as {@code REAL_64BITS} since we
     *         drop the prefix, but can't get a name starting with a digit.
     *
     * @see #REAL_32BITS
     * @see DataBuffer#TYPE_DOUBLE
     */
    public static final SampleDimensionType REAL_64BITS = new SampleDimensionType("REAL_64BITS");
    private final String name;

    /**
     * Constructs an enum with the given name. The new enum is
     * automatically added to the list returned by {@link #values}.
     *
     * @param name The enum name. This name must not be in use by an other enum of this type.
     */
    private SampleDimensionType(final String name){
        this.name = name;
    }

    /**
     * Returns the list of {@code SampleDimensionType}s.
     *
     * @return The list of codes declared in the current JVM.
     */
    public static SampleDimensionType[] values() {
        synchronized (VALUES) {
            return VALUES.toArray(new SampleDimensionType[VALUES.size()]);
        }
    }

}
