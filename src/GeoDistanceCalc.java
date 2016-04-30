
public final class GeoDistanceCalc 
{
	private GeoDistanceCalc() {}
	
	public static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		double d_phi = deg2rad(lat1) - deg2rad(lat2);
		double d_lambda = deg2rad(lon1) - deg2rad(lon2);
		double phi_m = (deg2rad(lat1) + deg2rad(lat2)) / 2.0;

		double dist = Math.sqrt(Math.pow(d_phi, 2) + Math.pow((Math.cos(phi_m) * d_lambda), 2));

		if (unit.equals("M"))
			dist = dist * 3958.761;
		else if (unit.equals("K"))
			dist = dist * 6371.009;
		else if (unit.equals("N"))
			dist = dist * 3440.069;

		return (dist);
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
