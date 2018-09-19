package HW5;

import java.text.ParseException;
import java.time.LocalDate;

public class Monthly extends Appointment{

	// Private member variables
	private LocalDate mstartDay;
	private LocalDate mendDay;
	private int dayOfMonth;
	
	public Monthly() {
		super(); //calling the constructor of the superclass
	}
	
	public Monthly(String description, LocalDate s, LocalDate e, int dm){
		super(description);
		mstartDay = s;
		mendDay = e;
		dayOfMonth = dm;
	} //constructor
	
	public LocalDate GetStartM() {
		return(mstartDay);
	}
	
	public LocalDate GetEndM() {
		return(mendDay);
	}
	
	public Integer GetDay() {
		return(dayOfMonth);
	}

	@Override
	boolean occursOn(LocalDate date) throws ParseException {
		//True if user enters a day that meets 3 requirements:
		//1. day equals to the day of the month
		//2. date occurs on or after the monthly appointment start day 
		//3. date occurs before the monthly appointment endDay
		boolean b1= (dayOfMonth ==  date.getDayOfMonth() && (date.isAfter(mstartDay) || date.equals(mstartDay)) && date.isBefore(mendDay));		
		return b1;
	}
	
}
