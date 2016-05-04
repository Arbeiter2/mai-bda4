
public final class GeoDistanceCalc 
{
	private GeoDistanceCalc() {}
	

	public static final double earthRadius_km = 6371.009;
	public static final double earthRadius_Nm = 3440.069;
	public static final double earthRadius_miles = 3958.761;
	
	
	
	/**
	 * Find the distance between two pairs of coordinatas A and B using haversine formula
	 * 
	 * @param lat1 - latitude of point A
	 * @param lon1 - longitude of point A
	 * @param lat2 - latitude of point B
	 * @param lon2 - longitude of point B
	 * @param unit - "M"=miles, "K"=km, "N"=nautical miles
	 * @return 
	 */
	public static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		double phi_1 = deg2rad(lat1);
		double phi_2 = deg2rad(lat2);
		
		double d_phi = deg2rad(lat1 - lat2);
		double d_lambda = deg2rad(lon1 - lon2);
		double a = Math.pow(Math.sin(d_phi/2) * Math.sin(d_phi/2), 2) +
		           Math.cos(phi_1) * Math.cos(phi_2) *
		           Math.pow(Math.sin(d_lambda/2), 2);
				
				
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		
		double dist = c * earthRadius_km;

		if (unit.equals("M"))
			dist = c * earthRadius_miles;
		else if (unit.equals("K"))
			dist = c * earthRadius_km;
		else if (unit.equals("N"))
			dist = c * earthRadius_Nm;

		return (dist);
	}
	
	/**
	 * return the initial bearing (in radians) of the path between two points
	 * @param line_lat1 - latitude of point A
	 * @param line_lon1 - longitude of point A
	 * @param line_lat2 - latitude of point B
	 * @param line_lon2 - longitude of point B
	 * @return initial bearing of path (radians)
	 */
	public static double bearing(double line_lat1, double line_lon1, double line_lat2, double line_lon2)
	{
		double lambda1 = deg2rad(line_lat1);
		double lambda2 = deg2rad(line_lat2);
		double phi1 = deg2rad(line_lon1);
		double phi2 = deg2rad(line_lon2);

		double y = Math.sin(lambda2-lambda1) * Math.cos(phi2);
		double x = Math.cos(phi1)*Math.sin(phi2) -
		        Math.sin(phi1)*Math.cos(phi2)*Math.cos(lambda2-lambda1);
		return Math.atan2(y, x);
	}

	/**
	 * finds the distance of a point C from the line joining two other points A and B
	 * @param line_lat1 - latitude of point A
	 * @param line_lon1 - longitude of point A
	 * @param line_lat2 - latitude of point B
	 * @param line_lon2 - longitude of point B
	 * @param point_lat - latitude of point C
	 * @param point_long - latitude of point C
	 * @param unit - "M"=miles, "K"=km, "N"=nautical miles
	 * @return
	 */
	public static double distanceFromLine(double line_lat1, double line_lon1, double line_lat2, double line_lon2, 
			double point_lat, double point_long, String unit) {
		
		double R = earthRadius_km;
		if (unit == "M")
			R = earthRadius_miles;
		else if (unit == "N")
			R = earthRadius_Nm;

		// find c, angular distance between the two points on the line
		double d_phi = deg2rad(line_lat1) - deg2rad(line_lat2);
		double d_lambda = deg2rad(line_lon1) - deg2rad(line_lon2);

		double a = Math.sin(d_phi/2) * Math.sin(d_phi/2) +
		        Math.cos(deg2rad(line_lat1)) * Math.cos(deg2rad(line_lat2)) *
		        Math.sin(d_lambda/2) * Math.sin(d_lambda/2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		
		double theta13 = bearing(line_lat1, line_lon1, point_lat, point_long);
		double theta12 = bearing(line_lat1, line_lon1, line_lat2, line_lon2);
		
		
		double dXt = Math.asin(Math.sin(c/R)*Math.sin(theta13-theta12)) * R;

		return (dXt);
	}	
	
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts decimal degrees to radians : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	public static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts radians to decimal degrees : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	public double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}

}
