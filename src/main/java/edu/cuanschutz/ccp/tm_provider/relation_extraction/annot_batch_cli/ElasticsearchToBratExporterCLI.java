package edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli;

import java.io.File;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(subcommands = { BatchCreateCommand.class, RepoStatsCommand.class })
public class ElasticsearchToBratExporterCLI implements Runnable {

	protected static final String ANNOTATION_DATA_DIR_NAME = "annotation-data";
	protected static final String BRAT_DIR_NAME = "brat";

	public static void main(String[] args) {
		int exitCode = new CommandLine(new ElasticsearchToBratExporterCLI()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public void run() {
		System.out.println("Base CLI running with no arguments....");
	}

	/**
	 * Given the base repository directory, return the annotation-data directory
	 * 
	 * @param baseDir
	 * @return
	 */
	protected static File getAnnotationDataDir(File baseDir) {
		File annotationDataDir = new File(baseDir, ANNOTATION_DATA_DIR_NAME);
		if (annotationDataDir.exists()) {
			return annotationDataDir;
		}
		System.err.println(
				"\n>>> User-supplied base directory does not contain a sub-directory of the name 'annotation-data'. "
						+ "Please check your base directory and retry. <<<");
		return null;
	}

	/**
	 * Given the base repository directory, return the annotation-data/brat
	 * directory
	 * 
	 * @param baseDir
	 * @return
	 */
	protected static File getBratDir(File baseDir) {
		File annotationDataDir = getAnnotationDataDir(baseDir);
		if (annotationDataDir != null) {
			File bratDir = new File(annotationDataDir, BRAT_DIR_NAME);
			if (bratDir.exists()) {
				return bratDir;
			}
			System.err.println(String.format("\n>>> %s/%s directory does not exist. Please retry. <<<",
					ANNOTATION_DATA_DIR_NAME, BRAT_DIR_NAME));
		}
		return null;
	}

	/**
	 * Given the base repository directory, return the annotation-data/brat/project
	 * directory
	 * 
	 * @param baseDir
	 * @return
	 */
	protected static File getAnnotProjectDir(File baseDir, String annotProjectName) {
		File bratDir = getBratDir(baseDir);
		if (bratDir != null) {
			File projectDir = new File(bratDir, annotProjectName);
			if (projectDir.exists()) {
				return projectDir;
			}
			System.err.println(String.format("\n>>> %s/%s/%s directory does not exist. Please retry. <<<",
					ANNOTATION_DATA_DIR_NAME, BRAT_DIR_NAME, annotProjectName));
		}
		return null;
	}

}
