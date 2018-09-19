package HW5;


import java.time.LocalDate;


public class Onetime extends Appointment{
	// Private member variables
	private LocalDate onetime;
	
	public Onetime() {
		super(); //calling the constructor of the superclass
	}
	
	public Onetime(String description, LocalDate date){
		super(description);
		onetime = date;
	}//constructor
	
	public LocalDate GetDate() {
		return(onetime);
	}
	
	//occursOn
	public boolean occursOn(LocalDate date1) {
		return date1.equals(onetime);
		}
	}
