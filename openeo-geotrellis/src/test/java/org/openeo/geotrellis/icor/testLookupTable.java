package org.openeo.geotrellis.icor;

import org.junit.Test;

public class testLookupTable {

	@Test
	public void testLookupTableRead() throws Exception{
		LookupTable lut=LookupTableIO.readLUT("test_lut");
	}
	
}
