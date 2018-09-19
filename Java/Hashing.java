package HW6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.lang.Math;

/**
 * Java Homework #6 on Hashing. 
 * The Hashing class implements three different
 * string hashing methods and outputs their performance.
 *
 * @author Jamie Chen
 * @author Brooke Kennedy
 * @since 2017-11-24
 */
public class Hashing{
	
	/**
	 * Method 1: h(X) = Sum(ASCII(char c)) % size
	 *
	 * @param names the String ArrayList of names from input file
	 * @return HashMap of hashed names
	 */
	public static HashMap<Integer, ArrayList<String>> hashFunction1(ArrayList<String> names){
		
		int size = names.size();
		
		// Hashmap to store key and names
		HashMap<Integer, ArrayList<String>> hmap = new HashMap<Integer, ArrayList<String>>();
		
		// Populate map with initial empty lists
		for(int i = 0; i < size; i++){
			ArrayList<String> names_list = new ArrayList<String>();
			hmap.put(i, names_list);
		}
		
		// Array to store ascii sum for each name
		int[] asciiSum = new int[size];
		
		// Loop through each name
		for(int i = 0; i < size; i++){
			// Loop through each character in each name
			for(int j = 0; j < names.get(i).length(); j++){
				// Sum the ascii values for each name
				asciiSum[i] += (int) names.get(i).charAt(j);
			}
		}
		
		// Calculate hash index and put it and corresponding name in hashmap
		for(int i = 0; i < size; i++){
			// Grab current value
			ArrayList<String> temp = hmap.get(asciiSum[i] % size);
			// Append name
			temp.add(names.get(i));
			// Add to map
			hmap.put(asciiSum[i] % size, temp);
		}
		return hmap;
	}
	
	/**
	 * Method 2: java built-in hashCode()
	 *
	 * @param names the String ArrayList of names from input file
	 * @return HashMap of hashed names
	 */
	public static HashMap<Integer, ArrayList<String>> hashFunction2(ArrayList<String> names){
		int size = names.size();
		// Hashmap2 to store key and names
		HashMap<Integer, ArrayList<String>> hmap2 = new HashMap<Integer, ArrayList<String>>();

		// Populate map with initial empty lists
		for(int i = 0; i < size; i++){
			ArrayList<String> names_list = new ArrayList<String>();
			hmap2.put(i, names_list);
		}
		
		// List to store hashCode for each name
		int[] hashC = new int[size];
		
		// Loop through each name
		for(int i = 0; i < size; i++){
			hashC[i] = Math.abs(names.get(i).hashCode() % size);
		}

		// Put hashCode and corresponding name in hashmap2
		for(int i = 0; i < size; i++){
			
			// Grab current value
			ArrayList<String> temp = hmap2.get(hashC[i]);
			
			//Initialize names_list
			if(temp == null) {
				ArrayList<String> names_list = new ArrayList <String> ();
				names_list.add(names.get(i));
				hmap2.put(hashC[i], names_list);
			}
			else {
				temp.add(names.get(i));
			}
		}
		return hmap2;
	}

	/**
	 * Method 3: new hash function using prime number
	 *
	 * @param names the String ArrayList of names from input file
	 * @return HashMap of hashed names
	 */
	public static HashMap<Integer, ArrayList<String>> hashFunction3(ArrayList<String> names){
		int size = names.size();
		// Hashmap3 to store key and names
		HashMap<Integer, ArrayList<String>> hmap3 = new HashMap<Integer, ArrayList<String>>();

		// Populate map with initial empty lists
		for(int i = 0; i < size; i++){
			ArrayList<String> names_list = new ArrayList<String>();
			hmap3.put(i, names_list);
		}
		
		// List to store hashCode for each name
		int[] hash3 = new int[size];
		
		// Loop through each name
		for(int i = 0; i < size; i++){
			int hash = 17;
			// Loop through each character in each name
			for(int j = 0; j < names.get(i).length(); j++){
				// Sum the values for each name
				hash += hash * 13 + (int) names.get(i).charAt(j);
				hash3[i] = Math.abs(hash % size);
			}
		}

		// Put hashCode and corresponding name in hashmap3
		for(int i = 0; i < size; i++){
			
			// Grab current value
			ArrayList<String> temp = hmap3.get(hash3[i]);
			
			//Initialize names_list
			if(temp == null) {
				ArrayList<String> names_list = new ArrayList <String> ();
				names_list.add(names.get(i));
				hmap3.put(hash3[i], names_list);
			}
			else {
				temp.add(names.get(i));
			}
			
		}
		return hmap3;

	}
	
	/**
	 * Writer function to output hashing results files
	 * 
	 * @param hmap hashmap of key and list of names
	 * @param fileName output files
	 * @param method hashing method using ascii, hashCode, and new hashing function
	 * @return String performance check on load factor using different hashing methods
	 */
	public static String writeToFile(HashMap<Integer, ArrayList<String>> hmap, String fileName, String method) {
		int counter = 0;
		int size = hmap.size();
		try {
			FileWriter writer = new FileWriter(fileName);
			
			for (Map.Entry<Integer,ArrayList<String>> entry : hmap.entrySet()) {
				int key = entry.getKey();
				String value = entry.getValue().toString();
				writer.write("Key: " + key +", ");
				if(entry.getValue().size() == 0){
					writer.write("EMPTY LINE\n\n");
				}
				else{
					writer.write ("Value: " + value + "\n\n" );	
					counter ++;					
				}
			} 
			writer.close(); 
			
		} catch(Exception e) {
			System.err.println(e);
			return null;
		}
		return ("Load factor using: "+ method + " = " + counter + "/" +size+"\n");
	}

	/**
	 * Writer function to output performance check to txt file
	 * 
	 * @param index_count array containing performance from hashing functions 
	 */	
	public static void performToFile (String[] index_count) {
		try {
			FileWriter writer = new FileWriter("Performance check.txt");
			for (int i = 0; i < 3; i++) {
				writer.write(index_count[i]);
			}
			writer.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
   /**
    * This is the main method which makes use of three different hashing methods.
    * @exception IOException On input error
    * @see IOException
    */
	public static void main(String[] args) throws IOException{
		
		// File to read in
		String pathname = "input.txt";
		File file = new File(pathname);
			
		// Declare array list to store names from file
		ArrayList<String> names200 = new ArrayList<String>();
		
		// Read file into the arraylist
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				names200.add(line);
			}
		}
		// Create array list of first 100 names
		ArrayList<String> names100 = new ArrayList<String>(names200.subList(0,100));
	    
		//performance metric on hashing methods
		int numberOfHashMethods = 3;
		String[] index_count = new String[numberOfHashMethods];
		
		// Ask the user to input size of ArrayList
		Scanner scanner = new Scanner(System.in);
		//catch menu selection non-integer input error
	    boolean flag = false;
		while (!flag) {
			try {
				System.out.println("\n***************************************************************");
				System.out.println("Please select size of the array:");
				System.out.println("1. 100");
				System.out.println("2. 200");
				System.out.println("0. Exit");
				System.out.println("***************************************************************\n");
				System.out.println("Please make a selection: ");
				
				int menu = scanner.nextInt();
				scanner.nextLine();
				
				if (menu == 0) {
					System.out.println("Thank you for participating!");
					break;
				}
				
				switch (menu) {
					case 1:
						index_count[0] = writeToFile(hashFunction1(names100), "output1_ascii.txt", "hash function using ASCII");
						index_count[1] = writeToFile(hashFunction2(names100), "output2_hashCode.txt", "hashCode function");
						index_count[2] = writeToFile(hashFunction3(names100), "output3_newhash.txt", "new hash function");
						performToFile(index_count);
						System.out.println("Output files generated!");
						break; 
					
					case 2:
						index_count[0] = writeToFile(hashFunction1(names200), "output1_ascii.txt", "hash function using ASCII");
						index_count[1] = writeToFile(hashFunction2(names200), "output2_hashCode.txt", "hashCode function");
						index_count[2] = writeToFile(hashFunction3(names200), "output3_newhash.txt", "new hash function");
						performToFile(index_count);
						System.out.println("Output files generated!");
						break; 
				}
			}
			catch (java.util.InputMismatchException e) {
				System.out.println("That was not a valid number, please try again: ");
				scanner.next();				
			}
		}//end of While loop
		scanner.close();
	}//end of main
}


