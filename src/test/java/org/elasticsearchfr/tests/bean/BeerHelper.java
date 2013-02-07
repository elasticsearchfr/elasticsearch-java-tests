package org.elasticsearchfr.tests.bean;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;


public class BeerHelper {

	public static Beer generate() {

		return new Beer(generateBrand(),
                generateColour(),
                Math.random()*2,
                Math.random()*10,
                generateDate());
	}

	private static String generateBrand() {

		Long result = Math.round(Math.random() * 2);

		switch (result.intValue()) {
		case 0:
			return "Heineken";
		case 1:
			return "Grimbergen";
		case 2:
			return "Kriek";
		default:
			break;
		}

		return null;
	}
	
	
	private static Colour generateColour() {

		Long result = Math.round(Math.random() * 2);

		switch (result.intValue()) {
		case 0:
			return Colour.DARK;
		case 1:
			return Colour.PALE;
		case 2:
			return Colour.WHITE;
		default:
			break;
		}

		return null;
	}

    private static final Date DATE_2010;
    private static final Date DATE_2011;
    private static final Date DATE_2012;

    static {
        Calendar cal = Calendar.getInstance();

        cal.set(2010, Calendar.JULY, 17);
        DATE_2010 = cal.getTime();

        cal.set(2011, Calendar.OCTOBER, 31);
        DATE_2011 = cal.getTime();

        cal.set(2012, Calendar.DECEMBER, 26);
        DATE_2012 = cal.getTime();


    }
    private static Date generateDate() {

        Long result = Math.round(Math.random() * 2);

        switch (result.intValue()) {
            case 0:
                return DATE_2010;
            case 1:
                return DATE_2011;
            case 2:
                return DATE_2012;
            default:
                break;
        }

        return null;
    }



    public static Beer toBeer(String json) throws JsonParseException, JsonMappingException, IOException {
		// instance a json mapper
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse
		Beer beer = mapper.readValue(json.getBytes(), Beer.class);
		return beer;

	}
}
