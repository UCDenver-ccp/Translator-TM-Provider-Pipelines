package edu.cuanschutz.ccp.tm_provider.etl.util;

/**
 * returns the version as specified in the META-INF/MANIFEST file. This version
 * mirrors the version in the Maven pom.xml file.
 *
 */
public class Version {

	private Version() {
	}

	public static String getProjectVersion() {
		return Version.class.getPackage().getImplementationVersion();
	}
}
