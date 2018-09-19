import java.util.Scanner;

public class currencyConvert {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hi");
		System.out.println("Please enter the price for one dollar in Japanese yen:");
		
	    //initiate scanner to take in exchange rate type double	
		Scanner sc = new Scanner(System.in);
		double rate = 0;
		boolean flag = false;
		
		//Repeat until a valid conversion input is entered
		while (!flag) {
			try {
				rate = sc.nextDouble();
				flag = true;
			}
			catch (java.util.InputMismatchException e) {
				System.out.println("That was not a valid number, please try again: ");
				sc.next();
			}
		}

		//selection menu			
		System.out.println("Please take a look at the following menu:" );
		System.out.println("****1. Convert one dollar to Japanese yen****");
		System.out.println("****2. Convert one Japanese yen to dollar****");
		System.out.println("****3. Change the conversion rate************");
		System.out.println("****0. Exit the menu*************************");
		
		int selection = 1;
		
		do {
			System.out.println("Make your selection:");
			
			//catch non-integer and out of range integer inputs
				try{
					selection = sc.nextInt();
					
					if (selection >3) {
						throw new IllegalArgumentException();
					}
					
					switch (selection){
					case 1:
						double usd2j = rate;
						System.out.println("One dollar yields Japanese yen of: "+ usd2j);
						break;
					case 2:
						double j2usd = 1/rate;
						System.out.println("One Japanese yen yields dollar amount of: "+ j2usd);
					   break;
					case 3:
						System.out.println("Please enter the new conversion rate for one dollar in Japanese yen:");
						rate = sc.nextDouble();
						break;
						} //end of switch
				}
				//catch inputs that are outside of the selection range
				catch(IllegalArgumentException e) {
					System.out.println("You entered an invalide selection, please try again and enter 0 if you wish to exit.");
				}
				//catch string inputs
				catch(java.util.InputMismatchException e) {
					System.out.println("That was not a number input, please try again: ");
					sc.next();
				}
				} while (selection!= 0); //end of loop
		System.out.println("Thank you for participating!");
		sc.close();
		}
	}

	