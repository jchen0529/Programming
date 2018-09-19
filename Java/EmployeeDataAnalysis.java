
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.*;


public class MyFileReader {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String pathname = "/Users/siliangchen/Desktop/Northwestern/Coursework/M422 Java and Python/WEEK 6/employees.txt";
		File file = new File(pathname);
		
		//Count number of lines in the file
		int n = 0;
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))){
			while((bufferedReader.readLine())!=null) {
				n++;
			}
		}
		
		//Declare arrays with size equal to the number of lines
		String[] names = new String[n];
		String[] dob = new String[n];
		int[] salary = new int[n];
		int i=0;
		
		//generate three arrays - names, dob, salary
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
					names[i] = line.split(",")[0];
					dob[i] = line.split(",")[1];
					salary[i] = Integer.parseInt(line.split(",")[2]);
					i++;
			}
		}
		
		//calculate the average employee salary
		double sum_sal = IntStream.of(salary).sum();
		double avg_sal = sum_sal/n;
		
		//Find the number of employees who make above the average
		int above_avg = 0;
		for (int counter = 0; counter < salary.length; counter++) {
			if (salary[counter] > avg_sal){
				above_avg ++;
			}
		}
		
		//Find the age of employees
		int item = 0;
		int year_now = 2017;
		int[]year = new int[n];
		
		for (int agecount = 0; agecount < n; agecount++) {
			item = Integer.parseInt(dob[agecount].split("/")[2]);
			year[agecount] = year_now - item;
		}
		
		//calculate the average employee age
		double sum_age = IntStream.of(year).sum();
		double avg_age = sum_age/n;
		
		//print out results
		System.out.println("Total number of employees in the file:" + n);
		
		//Find the maximum salary
		int max_sal = IntStream.of(salary).max().getAsInt();
		for (int a = 0; a < salary.length; a++) {
			if (salary[a] == max_sal){
				System.out.println("Employee that has the highest salary:" + names[a] + "," + max_sal);
			}
		}
		System.out.println("Average salary of all employees:" + avg_sal);
		System.out.println("The number of employees who make above the average:" + above_avg);
		System.out.println("Average age of employees:" + avg_age);
		
		//Sort employee names based on salary with two for loops
		for (int s=0; s< n; s++) {
			for (int j = 0; j < n; j++) {
				if (salary[s] < salary[j] && s<j){
					String temp1 = names[s];
					int temp2 = salary[s];
					//While the salary array is being sorted, the names elements are being moved the exact same way
					names[s] = names[j];
					salary[s] = salary[j];
					
					names[j] = temp1;
					salary[j]= temp2;
				}
			}
		}
		//Output employees file
		String[] writeLines = new String[n];
		for (int w = 0; w < writeLines.length; w++) {
			writeLines[w] = names[w];
		}
		
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(pathname.replace("txt", "csv")));
		bufferedWriter.write("Employee Names" +"\n");
		for (String line : writeLines) {
			bufferedWriter.write(line+"\n");
		}
		bufferedWriter.close();
		}
}

