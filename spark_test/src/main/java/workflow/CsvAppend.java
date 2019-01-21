package workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

public class CsvAppend {

	public static void main(String[] args) throws IOException {

		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setLineSeparator("\n");
		settings.getFormat().setDelimiter(',');
		settings.getFormat().setQuote('"');
		settings.getFormat().setQuoteEscape('\\');


		CsvParser parser = new CsvParser(settings);

		Writer fileWriter = new FileWriter("D:\\data_test\\root\\20180806_mem_usage.csv");

		CsvWriterSettings writerSettings = new CsvWriterSettings();
		CsvWriter writer = new CsvWriter(fileWriter, writerSettings);

		File files=new File("D:\\data_test\\root\\20180806_mem_usage");
		int i=0;
		for (File file : files.listFiles()) {
			if(!file.getName().endsWith("csv")) continue;
			i++;
//			if(i>3) {
//				break;
//			}
			parser.beginParsing(file,Charset.forName("UTF-8"));


			String[] dataArray =null;

			if(i>1) {
				dataArray=parser.parseNext();
			}

			while ((dataArray=parser.parseNext()) !=null) {

				writer.writeRow(dataArray);

			}

			writer.flush();
			System.out.println(i+" "+file.getName()+" finish");

		}

		writer.close();




	}

}

