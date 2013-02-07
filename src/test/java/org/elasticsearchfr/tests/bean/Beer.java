package org.elasticsearchfr.tests.bean;

import java.io.Serializable;
import java.util.Date;

public class Beer implements Serializable {
	private static final long serialVersionUID = 1L;

	private String brand;
	private Colour colour;
	private double size;
	private double price;
    private Date date;

	public Beer() {
	}

	/**
	 * @param brand
	 * @param colour
	 * @param size
	 * @param price
	 */
	public Beer(String brand, Colour colour, double size, double price, Date date) {
		this.brand = brand;
		this.colour = colour;
		this.size = size;
		this.price = price;
        this.date = date;
	}

	/**
	 * @return the brand
	 */
	public String getBrand() {
		return brand;
	}

	/**
	 * @param brand
	 *            the brand to set
	 */
	public void setBrand(String brand) {
		this.brand = brand;
	}

	/**
	 * @return the colour
	 */
	public Colour getColour() {
		return colour;
	}

	/**
	 * @param colour
	 *            the colour to set
	 */
	public void setColour(Colour colour) {
		this.colour = colour;
	}

	/**
	 * @return the size
	 */
	public double getSize() {
		return size;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(double size) {
		this.size = size;
	}

	/**
	 * @return the price
	 */
	public double getPrice() {
		return price;
	}

	/**
	 * @param price
	 *            the price to set
	 */
	public void setPrice(double price) {
		this.price = price;
	}

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
	public boolean equals(Object obj) {

		if (obj == null) return false;
		if (!(obj instanceof Beer)) return false;
		
		Beer beer = (Beer) obj;
		
		if (this.brand != beer.brand && this.brand != null && !this.brand.equals(beer.brand)) return false;
		
		if (this.colour != beer.colour) return false;
		if (this.size != beer.size ) return false;
		if (this.price != beer.price) return false;
        if (this.date != beer.date && this.date != null && !this.date.equals(beer.date)) return false;

        return true;
	}
}
