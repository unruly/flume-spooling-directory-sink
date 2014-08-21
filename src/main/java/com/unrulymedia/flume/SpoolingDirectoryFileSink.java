package com.unrulymedia.flume;

import java.lang.reflect.Field;

import org.apache.flume.sink.RollingFileSink;

public class SpoolingDirectoryFileSink extends RollingFileSink {

	private SpoolingDirectoryPathManager spoolingDirectoryPathManager;

	public SpoolingDirectoryFileSink() throws NoSuchFieldException, IllegalAccessException {
		Field pathManagerField = RollingFileSink.class.getDeclaredField("pathController");
		pathManagerField.setAccessible(true);
		spoolingDirectoryPathManager = new SpoolingDirectoryPathManager();
		pathManagerField.set(this, spoolingDirectoryPathManager);
	}
	
	@Override
	public void stop() {
		super.stop();
		spoolingDirectoryPathManager.rotate();
	}

}
