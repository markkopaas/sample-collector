# sample-collector
Example project demonstrating use of streams, promises and queues

Project inspiration came from statistical test sample collection project, presented in a [GTAC talk](http://www.youtube.com/watch?v=oGSMiR46bkw&t=00h11m40s)

The solution is implemented as an promise based filter with local storage.
We take data sample (object) and analyze all of its properties.
If a sample has some of the properties sufficiently distinct from any previous data samples, filter responds 'true';
If none of the properties of the data sample are distinct, filter responds 'false';

It is also possible to specify interdependent properties as "features". This means that any different combination of the interdependent properties is considered a distinct "feature class"

Further ideas:
 - collect statistics on samples
 - export data collection
 - instead of serializing the property lookup, it might make sense to allow some duplicates and enable parallel lookup for speed.
 - flexible network storage would allow parallel processing from multiple nodes
