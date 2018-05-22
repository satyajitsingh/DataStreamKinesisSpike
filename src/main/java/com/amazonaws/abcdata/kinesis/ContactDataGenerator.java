package com.amazonaws.abcdata.kinesis;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.ABC.Contact;

public class ContactDataGenerator {	
	private static final List<Contact> contact_list = new ArrayList<Contact>();
	private static DateTimeFormatter dstf = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss",Locale.UK);
	public static final List<Contact> addContact(int noRec, int startFrom) {
		contact_list.clear();
		for(int i = 1; i<=noRec; i++) {			
	    	
	    	String toPort0 = "Aleppo Syria";
	    	String toPort1 = "Moscow Russia";
	    	String toPort2 = "Beijing China";
	    	String toPort3 = "Colombo SriLanka";
	    	String toPort4 = "NewDelhi India";
	    	String fromPort1 = "Heathrow London";
	    	String fromPort2 = "Luton London";
	    	String fromPort3 = "Birmingham";
	    	String fromPort4 = "Bristol";
	    	LocalDateTime initDate = LocalDateTime.now();
	    	Contact contact = new Contact();	    	
	    	try {
		    	if (i % 2 == 0 ) {
		    		 contact = new Contact(startFrom,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort1, fromPort1);
		    	}
		    	else if(i % 3 == 0) {
		    		 contact = new Contact(startFrom,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort2, fromPort2);
		    	}
		    	else if(i % 4 == 0) {
		    		 contact = new Contact(startFrom,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort3, fromPort3);
		    	}
		    	else if(i % 5 == 0) {
		    		 contact = new Contact(startFrom,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort4, fromPort4);
		    		initDate = initDate.plusDays(5);
		    	}
		    	else {
		    		contact = new Contact(startFrom,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort0, fromPort1);
		    	}	
		    	startFrom++;
		    	contact_list.add(contact);
	    	}catch (Exception e) {
 			e.printStackTrace();
	    	}
		}
		return contact_list;
	}
}
