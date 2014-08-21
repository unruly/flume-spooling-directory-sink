package com.unrulymedia.flume;

import org.apache.flume.FlumeException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SpoolingDirectoryPathManagerTest {

	private String baseDirectory = "./tmp/baseDirectory";
	private File baseDir = new File(baseDirectory);
	private File workingDirectory = new File(baseDir, "working");

	@Before
	public void makeBaseDirectory() throws IOException {
		forceMkdir(baseDir);
		cleanDirectory(baseDir);
	}

	@Test
	public void shouldCreateTheWorkingDirectoryIfItDoesNotExist() throws Exception {
		assertThat(workingDirectory, not(isDirectory()));

		new SpoolingDirectoryPathManager().setBaseDirectory(baseDir);

		assertThat(workingDirectory, isDirectory());
	}

	@Test
	public void shouldDoNothingIfTheWorkingDirectoryExists() throws Exception {
		forceMkdir(workingDirectory);
		assertThat(workingDirectory, isDirectory());

		new SpoolingDirectoryPathManager().setBaseDirectory(baseDir);

		assertThat(workingDirectory, isDirectory());
	}

	@Test(expected = FlumeException.class)
	public void shouldThrowAFlumeExceptionIfWorkingDirectoryIsNotEmpty() throws Exception {
		forceMkdir(workingDirectory);
		File dirtyFile = new File(workingDirectory, "foo");
		assertTrue(dirtyFile.createNewFile());

		new SpoolingDirectoryPathManager().setBaseDirectory(baseDir);
	}

	@Test
	public void getNextFileShouldReturnAFileInTheWorkingDirectory() throws Exception {
		SpoolingDirectoryPathManager pathManager = new SpoolingDirectoryPathManager();
		pathManager.setBaseDirectory(baseDir);

		File nextFile = pathManager.nextFile();

		assertThat(nextFile.getParentFile(), is(workingDirectory));
	}
	
	@Test
	public void getCurrentShouldReturnTheSameFileInTheWorkingDirectoryEachTimeItIsCalled() throws Exception {
		SpoolingDirectoryPathManager pathManager = new SpoolingDirectoryPathManager();
		pathManager.setBaseDirectory(baseDir);

		File currentFile = pathManager.getCurrentFile();
		assertThat(currentFile.getParentFile(), is(workingDirectory));
		assertThat(pathManager.getCurrentFile(), sameInstance(currentFile));
	}

	@Test
	public void shouldMoveFileToBaseDirectoryOnRotate() throws Exception {
		SpoolingDirectoryPathManager pathManager = new SpoolingDirectoryPathManager();
		pathManager.setBaseDirectory(baseDir);

        File originalFile = pathManager.getCurrentFile();
		String expectedContent = "i am a teapot";

        write(originalFile, expectedContent);
		
		pathManager.rotate();
		
		File expectedFile = new File(baseDir,originalFile.getName());

        assertThat(expectedFile.exists(), is(true));
		assertThat(readFileToString(expectedFile), is(expectedContent));
		assertThat(originalFile.exists(), is(false));
		
		File newFile = pathManager.getCurrentFile();

        assertThat(newFile, not(originalFile));
	}
	
	private Matcher<File> isDirectory() {
		return new TypeSafeMatcher<File>() {

			@Override
			public void describeTo(Description description) {
				description.appendText(" a directory which exists");
			}

			@Override
			protected boolean matchesSafely(File dir) {
				return dir.exists() && dir.isDirectory();
			}
		};
	}

}