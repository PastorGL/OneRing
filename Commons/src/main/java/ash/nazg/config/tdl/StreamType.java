package ash.nazg.config.tdl;

public enum StreamType {
    /**
     * This type represents the case if any other non-special RDD types except {@link #KeyValue} are allowed
     */
    Plain,
    /**
     * The underlying CSV RDD a collection of rows with a flexibly defined set of columns
     */
    CSV,
    /**
     * For output CSV RDDs with a fixed set of columns
     */
    Fixed,
    /**
     * For RDDs consisting of Point objects
     */
    Point,
    /**
     * For RDDs consisting of Track objects
     */
    Track,
    /**
     * For RDDs consisting of Polygon objects
     */
    Polygon,
    /**
     * For PairRDDs, each record of whose is a key and value pair
     */
    KeyValue,
    /**
     * This special type is allowed only for the output of filter-like operations that have exactly one positional
     * input (of any type), and just throw out some records as a whole, never changing records that pass the filter
     */
    Passthru
}
