package HW5;

import java.time.LocalDate;

public class Daily extends Appointment{

	// Private member variables
	private LocalDate startDay;
	private LocalDate endDay;
	
	
	public Daily() {
		super(); //calling the constructor of the superclass
	}

	public Daily(String description, LocalDate start, LocalDate end){
		super(description);
		startDay = start;
		endDay = end;
	}//constructor
	
	public LocalDate GetStartD() {
		return(startDay);
	}
	
	public LocalDate GetEndD() {
		return(endDay);
	}
	
	@Override
	boolean occursOn(LocalDate date) {
		//True if user enters a day that is on or after the start day and before the endDay
		return((( date.isAfter(startDay) || date.isEqual(startDay)) && date.isBefore(endDay)));
	}
}
