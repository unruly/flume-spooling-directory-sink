package com.unrulymedia.flume;

import org.junit.Test;

public class SpoolingDirectoryFileSinkTest {

	@Test
	public void shouldNotErrorOnConstruction() throws Exception {
		new SpoolingDirectoryFileSink();
	}
	
}
