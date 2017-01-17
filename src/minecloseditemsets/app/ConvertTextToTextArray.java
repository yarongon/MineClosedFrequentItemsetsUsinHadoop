package minecloseditemsets.app;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.DataOutputBuffer;

import minecloseditemsets.hadoop.io.TextArrayWritable;

public class ConvertTextToTextArray {

	public static void main(String[] args) throws IOException {
		String inputFilename = args[0];
		String outputFilename = args[1];
		
		BufferedReader in = new BufferedReader(new FileReader(inputFilename));
		OutputStream out = new FileOutputStream(outputFilename); 
		DataOutputBuffer outBuffer = new DataOutputBuffer();
		TextArrayWritable outArray = new TextArrayWritable();
		
		String line;
		while ((line = in.readLine()) != null) {
			String[] inArray = line.split(" ");
			outArray.setArray(inArray);
			outArray.write(outBuffer);
		}
		in.close();
		outBuffer.writeTo(out);
		outBuffer.close();

	}

}
