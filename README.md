# sample-collector
Example project demonstrating use of streams, promises and queues

Project inspiration came from statistical test sample collection project, presented in a [GTAC talk](http://www.youtube.com/watch?v=oGSMiR46bkw&t=00h11m40s)

The solution is implemented as a promise based filter with local storage.
We take data sample (object) and analyze all of its properties.
If a sample has some of the properties sufficiently distinct from the previous data samples, filter responds 'true';
If none of the properties of the data sample are distinct, filter responds 'false';

It is possible to specify interdependent properties as "features". This means that any different combination of the interdependent properties is considered a distinct "feature class"

Group identifier expression is useful for creating property classes separately for each group. For example, we might want to record distict parameter sets separetely for each script. In this case, script name is our grouping expression.

See `example.js` for usage example for parsing apache log and filtering out distinct parameter sets.

Further ideas:
 - collect statistics on samples
 - export data collection
 - instead of serializing the property lookup, it might make sense to allow some duplicates and enable parallel lookup for speed.
 - flexible network storage would allow parallel processing from multiple nodes
