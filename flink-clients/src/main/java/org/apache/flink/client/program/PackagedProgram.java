package org.apache.flink.client.program;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.JarUtils;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.apache.flink.client.program.PackagedProgramUtils.isPython;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates represents a program, packaged in a jar file. It supplies
 * functionality to extract nested libraries, search for the program entry point, and extract
 * a program plan.
 */
public class PackagedProgram {

	/**
	 * Property name of the entry in JAR manifest file that describes the Flink specific entry point.
	 */
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";

	/**
	 * Property name of the entry in JAR manifest file that describes the class with the main method.
	 */
	public static final String MANIFEST_ATTRIBUTE_MAIN_CLASS = "Main-Class";

	// --------------------------------------------------------------------------------------------

	private final URL jarFile;

	private final String[] args;

	private final Class<?> mainClass;

	private final List<File> extractedTempLibraries;

	private final List<URL> classpaths;

	private final ClassLoader userCodeClassLoader;

	private final SavepointRestoreSettings savepointSettings;

	/**
	 * Flag indicating whether the job is a Python job.
	 */
	private final boolean isPython;

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * arguments. For generating the plan the class defined in the className parameter
	 * is used.
	 *
	 * @param jarFile             The jar file which contains the plan.
	 * @param classpaths          Additional classpath URLs needed by the Program.
	 * @param entryPointClassName Name of the class which generates the plan. Overrides the class defined
	 *                            in the jar file manifest.
	 * @param configuration       Flink configuration which affects the classloading policy of the Program execution.
	 * @param args                Optional. The arguments used to create the pact plan, depend on
	 *                            implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException This invocation is thrown if the Program can't be properly loaded. Causes
	 *                                    may be a missing / wrong class or manifest files.
	 */
	private PackagedProgram(
			@Nullable File jarFile,
			List<URL> classpaths,
			@Nullable String entryPointClassName,
			Configuration configuration,
			SavepointRestoreSettings savepointRestoreSettings,
			String... args) throws ProgramInvocationException {
		this.classpaths = classpaths;
		this.savepointSettings = savepointRestoreSettings;
		this.args = args;


		// whether the job is a Python job.
		this.isPython = isPython(entryPointClassName);

		// load the jar file if exists
		this.jarFile = loadJarFile(jarFile);

		assert this.jarFile != null || entryPointClassName != null;

		// now that we have an entry point, we can extract the nested jar files (if any)
		this.extractedTempLibraries = 
			this.jarFile == null ? 
			Collections.emptyList() : 
			extractContainedLibraries(this.jarFile);

		this.userCodeClassLoader = ClientUtils.buildUserCodeClassLoader(
			getJobJarAndDependencies(),
			classpaths,
			getClass().getClassLoader(),
			configuration);

		// load the entry point class
		this.mainClass = loadMainClass(
			// if no entryPointClassName name was given, we try and look one up through the manifest
			entryPointClassName != null ? entryPointClassName : getEntryPointClassNameFromJar(this.jarFile),
			userCodeClassLoader);
	}

	public SavepointRestoreSettings getSavepointSettings() {
		return savepointSettings;
	}

	public String[] getArguments() {
		return this.args;
	}

	public String getMainClassName() {
		return this.mainClass.getName();
	}

	/**
	 * Returns the description provided by the Program class. This
	 * may contain a description of the plan itself and its arguments.
	 *
	 * @return The description of the PactProgram's input parameters.
	 * @throws ProgramInvocationException This invocation is thrown if the Program can't be properly loaded. Causes
	 *                                    may be a missing / wrong class or manifest files.
	 */
	@Nullable
	public String getDescription()  {
		if (ProgramDescription.class.isAssignableFrom(this.mainClass)) {

			ProgramDescription descr = InstantiationUtil.instantiate(
					this.mainClass.asSubclass(ProgramDescription.class), ProgramDescription.class);
			
			return descr.getDescription();
		} 
	}

	/**
	 * This method assumes that the context environment is prepared, or the execution
	 * will be a local execution by default.
	 */
	public void invokeInteractiveModeForExecution() throws ProgramInvocationException {
		callMainMethod(mainClass, args);
	}

	/**
	 * Returns the classpaths that are required by the program.
	 *
	 * @return List of {@link java.net.URL}s.
	 */
	public List<URL> getClasspaths() {
		return this.classpaths;
	}

	/**
	 * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
	 *
	 * @return The user code ClassLoader.
	 */
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}

	/**
	 * Returns all provided libraries needed to run the program.
	 */
	public List<URL> getJobJarAndDependencies() {
		List<URL> libs = new ArrayList<URL>(this.extractedTempLibraries.size() + 1);

		if (jarFile != null) {
			libs.add(jarFile);
		}
		for (File tmpLib : this.extractedTempLibraries) {
			libs.add(tmpLib.getAbsoluteFile().toURI().toURL());
		}

		if (isPython) {
			libs.add(PackagedProgramUtils.getPythonJar());
		}

		return libs;
	}

	/**
	 * Deletes all temporary files created for contained packaged libraries.
	 */
	public void deleteExtractedLibraries() {
		deleteExtractedLibraries(this.extractedTempLibraries);
		this.extractedTempLibraries.clear();
	}

	private static boolean hasMainMethod(Class<?> entryClass) {
		Method mainMethod = entryClass.getMethod("main", String[].class);

		return Modifier.isStatic(mainMethod.getModifiers()) && Modifier.isPublic(mainMethod.getModifiers());
	}

	private static void callMainMethod(Class<?> entryClass, String[] args) throws ProgramInvocationException {
		assert (Modifier.isPublic(entryClass.getModifiers()));

		Method mainMethod = entryClass.getMethod("main", String[].class);
		

		assert (Modifier.isStatic(mainMethod.getModifiers())) ;

		assert (Modifier.isPublic(mainMethod.getModifiers()));

		mainMethod.invoke(null, (Object) args);
	}

	private static String getEntryPointClassNameFromJar(URL jarFile) throws ProgramInvocationException {

		JarFile jar = new JarFile(new File(jarFile.toURI()));


		Manifest manifest = jar.getManifest();

		Attributes attributes = manifest.getMainAttributes();

		// check for a "program-class" entry first
		String className = attributes.getValue(PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
		if (className != null) {
			return className;
		}

		// check for a main class
		className = attributes.getValue(PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS);
		if (className != null) {
			return className;
		} 
	}

	@Nullable
	private static URL loadJarFile(File jar) throws ProgramInvocationException {
			URL jarFileUrl = jar.getAbsoluteFile().toURI().toURL();

			checkJarFile(jarFileUrl);

			return jarFileUrl;
	}

	private static Class<?> loadMainClass(String className, ClassLoader cl) throws ProgramInvocationException {
		ClassLoader contextCl = null;
			contextCl = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(cl);
			return Class.forName(className, false, cl);
		
	}

	/**
	 * Takes all JAR files that are contained in this program's JAR file and extracts them
	 * to the system's temp directory.
	 *
	 * @return The file names of the extracted temporary files.
	 * @throws ProgramInvocationException Thrown, if the extraction process failed.
	 */
	public static List<File> extractContainedLibraries(URL jarFile) throws ProgramInvocationException {
		final JarFile jar = new JarFile(new File(jarFile.toURI()));

		final List<JarEntry> containedJarFileEntries = getContainedJarEntries(jar);
		if (containedJarFileEntries.isEmpty()) {
			return Collections.emptyList();
		}

			final List<File> extractedTempLibraries = new ArrayList<>(containedJarFileEntries.size());
	
			final Random rnd = new Random();
			final byte[] buffer = new byte[4096];

			for (final JarEntry entry : containedJarFileEntries) {
				// '/' as in case of zip, jar
				// java.util.zip.ZipEntry#isDirectory always looks only for '/' not for File.separator
				final String name = entry.getName().replace('/', '_');
				final File tempFile = copyLibToTempFile(name, rnd, jar, entry, buffer);
				extractedTempLibraries.add(tempFile);
			}

			return extractedTempLibraries;
		
	}

	private static File copyLibToTempFile(String name, Random rnd, JarFile jar, JarEntry input, byte[] buffer) throws ProgramInvocationException {
		final File output = createTempFile(rnd, input, name);
		try (
				final OutputStream out = new FileOutputStream(output);
				final InputStream in = new BufferedInputStream(jar.getInputStream(input))
		) {
			int numRead = 0;
			while ((numRead = in.read(buffer)) != -1) {
				out.write(buffer, 0, numRead);
			}
			return output;
		} 
	}

	private static File createTempFile(Random rnd, JarEntry entry, String name) {
			final File tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
			tempFile.deleteOnExit();
			return tempFile;
	}

	private static List<JarEntry> getContainedJarEntries(JarFile jar) {
		return jar.stream()
				.filter(jarEntry -> {
					final String name = jarEntry.getName();
					return name.length() > 8 && name.startsWith("lib/") && name.endsWith(".jar");
				})
				.collect(Collectors.toList());
	}

	private static void deleteExtractedLibraries(List<File> tempLibraries) {
		for (File f : tempLibraries) {
			f.delete();
		}
	}

	private static void checkJarFile(URL jarfile) throws ProgramInvocationException {
		JarUtils.checkJarFile(jarfile);
	}

	/**
	 * A Builder For {@link PackagedProgram}.
	 */
	public static class Builder {

		@Nullable
		private File jarFile;

		@Nullable
		private String entryPointClassName;

		private String[] args = new String[0];

		private List<URL> userClassPaths = Collections.emptyList();

		private Configuration configuration = new Configuration();

		private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

		public Builder setJarFile(@Nullable File jarFile) {
			this.jarFile = jarFile;
			return this;
		}

		public Builder setUserClassPaths(List<URL> userClassPaths) {
			this.userClassPaths = userClassPaths;
			return this;
		}

		public Builder setEntryPointClassName(@Nullable String entryPointClassName) {
			this.entryPointClassName = entryPointClassName;
			return this;
		}

		public Builder setArguments(String... args) {
			this.args = args;
			return this;
		}

		public Builder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings) {
			this.savepointRestoreSettings = savepointRestoreSettings;
			return this;
		}

		public PackagedProgram build() throws ProgramInvocationException {
			
			return new PackagedProgram(
				jarFile,
				userClassPaths,
				entryPointClassName,
				configuration,
				savepointRestoreSettings,
				args);
		}

		private Builder() {
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}
}
