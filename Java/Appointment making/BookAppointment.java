package HW5;

import java.util.Scanner;
import java.text.ParseException;
import java.util.ArrayList;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.io.FileWriter;
import java.io.IOException;


public class BookAppointment {

	public static void main(String[] args) throws ParseException {
		Scanner scanner = new Scanner(System.in);
		
		//day formatter for appointment date
		DateTimeFormatter dayformat = DateTimeFormatter.ofPattern("MM/dd/yyyy");
		
		//Array list that stores all the added appointments 
	    ArrayList<Appointment> appts = new ArrayList<Appointment>();
	    
	    //catch menu selection non-integer input error
	    boolean flag = false;
	    
		while (!flag) {
			try {
				System.out.println("***************************************************************");
				System.out.println("Welcome to your appointment booking. Please see menu below:");
				System.out.println("1. Add new appointment");
				System.out.println("2. Enter a date to check if appointment occurs and view appointments");
				System.out.println("3. Save appointments and export to file");
				System.out.println("0. Exit");
				System.out.println("***************************************************************");
				System.out.println("Please make a selection: ");
				
				int menu = scanner.nextInt();
				scanner.nextLine();
				if (menu == 0) {
					System.out.println("Thank you for participating!");
					break;
				}
				
				switch (menu) {
					case 1: 
						System.out.println("Please enter number of appointment type: 1.Onetime  2.Daily  3.Monthly");
						int typeselect = scanner.nextInt();
						scanner.nextLine();
						
						System.out.println("Please enter description:");
						String desc = scanner.nextLine();
						
						//one-time appointment
						if (typeselect == 1) {
							
							boolean datecheck = false;
							
							while (!datecheck){
								System.out.println("Please enter date (mm/dd/yyyy):");
								String str = scanner.nextLine();
								try {
									Onetime onetime = new Onetime(desc, LocalDate.parse(str,dayformat));
									appts.add(onetime);
									System.out.println("You've successfully added an one-time appointment on: " + onetime.GetDate() + "for" + onetime.GetDesc());
									System.out.println("appts length" + appts.size());
									datecheck = true;
								}
								
								catch(DateTimeParseException e){
									System.out.println("Invalid date input, please enter the date again in the format of mm/dd/yyyy");
								}
							}
						}
						
						//Daily appointment
						if (typeselect ==2) {
							boolean datecheck1 = false;
							while (!datecheck1){
								System.out.println("Please enter daily appointment start date (mm/dd/yyyy):");
								String start = scanner.nextLine();
								System.out.println("Please enter daily appointment end date (mm/dd/yyyy):");
								String end = scanner.nextLine();
								try {
									Daily dailyappt = new Daily(desc, LocalDate.parse(start,dayformat), LocalDate.parse(end,dayformat));
									System.out.println("You've successfully added a daily appointment between: " + dailyappt.GetStartD() + " and " + dailyappt.GetEndD() + "for" + dailyappt.GetDesc());
									appts.add(dailyappt);
									System.out.println("appts length" + appts.size());
									datecheck1 = true;
								}
								
								catch(DateTimeParseException e){
									System.out.println("Invalid date input, please enter the date again in the format of mm/dd/yyyy");
								}
							}
						}
						
						//Monthly appointment
						if (typeselect ==3) {
							boolean datecheck2 = false;
							while (!datecheck2){
								System.out.println("Please enter monthly appointment start date (mm/dd/yyyy):");
								String mstart = scanner.nextLine();
								System.out.println("Please enter monthly appointment end date (mm/dd/yyyy):");
								String mend = scanner.nextLine();
								System.out.println("Please enter which day of the month you want to schedule your appointment (dd):");
								int dom = scanner.nextInt();
								
								//check input day must be smaller than 31
								if (dom >31) {
									System.out.println("Invalid date input, you must enter a valid day of the month (between 1 - 31)");
									break;
								}
								
								try {
									Monthly mappt = new Monthly(desc, LocalDate.parse(mstart,dayformat), LocalDate.parse(mend,dayformat), dom);
									System.out.println("You've successfully added a monthly appointment between: " + mappt.GetStartM() + " and " + mappt.GetEndM() + " for " + mappt.GetDesc() +"On" +mappt.GetDay());
									appts.add(mappt);
									System.out.println("appts length" + appts.size());
									datecheck2 = true;
								}
								
								catch(DateTimeParseException e){
									System.out.println("Invalid date input, please enter the date again in the format of mm/dd/yyyy");
								}
							}
						}

					break;//end of case 1 add an appointment
					
					//take user input and print out all appointments that occur on that date
					case 2:
						System.out.println("Please enter date to check (mm/dd/yyyy): ");
						String datetocheck = scanner.nextLine();
						LocalDate tocheck = LocalDate.parse(datetocheck, dayformat);
						
						int count = 0;
						for (Appointment appt:appts) {
							if (appt.occursOn(tocheck)) {
								System.out.println(appt.toString());
								count++;
							}
						}
						if (count ==0) {
							System.out.println("No appointment found on this day.");
						}
					break;
					
					//saves and export to txt file
					case 3:
						
						FileWriter writer = null;
						try {
							writer = new FileWriter("appt.txt");
							for (Appointment appt: appts) {
								//output one-time appointments
								if (Onetime.class.isInstance(appt)) {
									Onetime temp = (Onetime) appt;
									writer.write("One-time appointment:");
									writer.write(temp.GetDesc());
									writer.write("\n");
									writer.write("Appointment Date:");
									writer.write(temp.GetDate().toString());
									writer.write("\n");
								}
								//output daily appointments
								if (Daily.class.isInstance(appt)) {
									Daily temp = (Daily) appt;
									writer.write("Daily appointment:");
									writer.write(temp.GetDesc());
									writer.write("\n");
									writer.write("Start Date:");
									writer.write(temp.GetStartD().toString());
									writer.write("\n");
									writer.write("End Date:");
									writer.write(temp.GetEndD().toString());
									writer.write("\n");
								}
								//output monthly appointments
								if (Monthly.class.isInstance(appt)) {
									Monthly temp = (Monthly) appt;
									writer.write("Monthly appointment:");
									writer.write(temp.GetDesc());
									writer.write("\n");
									writer.write("Start Date:");
									writer.write(temp.GetStartM().toString());
									writer.write("\n");
									writer.write("End Date:");
									writer.write(temp.GetEndM().toString());
									writer.write("\n");
									writer.write("Date of each month:");
									writer.write(temp.GetDay().toString());
									writer.write("\n");
								}
							}
						}
						catch(Exception e)
						{
							System.out.println(e.toString());
						}
						
						finally 
						{
							if (writer != null)  
				                try {  
				                	writer.close();  
				                	System.out.println("Existing appointments have been saved and exported as 'appt.txt'.");
				                } 
								catch (IOException e) {  
				                    throw new RuntimeException("Close failed.");  
				                }  
				        }  
					break; //end of file writer
				}//end of switch
			}//end of try
			//catch non-integer menu selection
			catch (java.util.InputMismatchException e) {
				System.out.println("That was not a valid number, please try again: ");
				scanner.next();
			}	
		}//end of while
	scanner.close();
	}// main
}//class
