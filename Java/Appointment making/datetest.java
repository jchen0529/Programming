package HW5;

import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import java.util.Scanner;

public class datetest {


	public static void main(String[] args) throws ParseException {
		
		Scanner scanner = new Scanner(System.in);
		System.out.println("enter date");
		String date = scanner.nextLine();
		
		DateTimeFormatter dayformat = DateTimeFormatter.ofPattern("MM/dd/yyyy");
		
		LocalDate  cd1 = LocalDate.parse(date, dayformat);
		System.out.println(cd1);
		
//		System.out.println("enter date");
//		String date2 = scanner.nextLine();
//		LocalDate cd2 = LocalDate.parse(date2, dayformat);
//		System.out.println(cd2);
		
		System.out.println( cd1.getDayOfMonth());
		
		scanner.close();
	}

}
