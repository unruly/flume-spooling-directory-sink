package com.unrulymedia.flume;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.flume.FlumeException;
import org.apache.flume.formatter.output.PathManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoolingDirectoryPathManager extends PathManager {

	private static final Logger LOG = LoggerFactory.getLogger(SpoolingDirectoryPathManager.class);
	
    private final PathManager delegate = new PathManager();
    
    private File workingDirectory;
    private File baseDirectory;

    @Override
    public void setBaseDirectory(File baseDirectory) {
	this.baseDirectory = baseDirectory;
		try {
		    workingDirectory = new File(baseDirectory, "working");
		    FileUtils.forceMkdir(workingDirectory);
		    if (workingDirectory.listFiles().length > 0) {
			throw new FlumeException("Working directory [" + workingDirectory
				+ "] is not empty. Please clean it and restart");
		    }
		    delegate.setBaseDirectory(workingDirectory);
		} catch (IOException e) {
			throw new FlumeException(e);
		}
    }

    @Override
    public File nextFile() {
		return delegate.nextFile();
    }
    
    @Override
    public File getCurrentFile() {
    	return delegate.getCurrentFile();
    }
    
    @Override
    public void rotate() {
    	File workingFile = getCurrentFile();
    	LOG.debug("Rotating current file: {}", workingFile);
    	delegate.rotate();
    	try {
			FileUtils.moveToDirectory(workingFile, baseDirectory, false);
		} catch (IOException e) {
			throw new FlumeException(e);
		}
    }
    
    @Override
    public File getBaseDirectory() {
    	return delegate.getBaseDirectory();
    }
    
    @Override
    public AtomicInteger getFileIndex() {
    	return delegate.getFileIndex();
    }
    
    @Override
    public long getSeriesTimestamp() {
    	return delegate.getSeriesTimestamp();
    }
    
}
