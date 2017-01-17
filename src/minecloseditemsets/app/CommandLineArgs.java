package minecloseditemsets.app;

import com.sampullara.cli.Argument;

public class CommandLineArgs {
	
	@Argument(value = "outputPath", alias = "o", description = "Output path", required = true)
	String outputPathName;

	@Argument(value = "minSup", alias = "m", description = "Minimum support", required = true)
	Double minSupport;
	
	@Argument(value = "inputFile", alias = "i", description = "input file name - used mainly for debug")
	String inputFileName;

}
