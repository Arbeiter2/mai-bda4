import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Delano Greenidge
 *
 * This class is dodgy as hell. It gets the output from the mapper, and builds a linked list from the 
 * CabTripSegmens it receives.
 */
@SuppressWarnings("rawtypes")
public class CabIDCombiner extends Reducer {
}
