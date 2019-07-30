//package org.myorg;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

public class ClusteringCoefficient {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(""Usage: WordCount <input-path> <output-path>"" );
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Δημιουργία context αντικειμένου και του πρώτου RDD
        //από το αρχείο email-Enron που βρίσκεται στο input-path
        SparkConf sparkConf = new SparkConf().setAppName("CCFINDER");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(inputPath);

        // PairFlatMapFunction<T, K, V>
        // T => Iterable<Tuple2<K, V>>
        JavaPairRDD<Long,Long> edges = lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long,Long>> call(String s) {
                String nodes[] = s.split(" ");
                long start = Long.parseLong(nodes[0]);
                long end = Long.parseLong(nodes[1]);
                //Οι ακμές του γράφου πρέπει να είναι μη κατευθυνόμενες. Υποθέτουμε ότι κάθε κόμβος
                // στέλνει και λαμβάνει e-mail από τον ίδιο κόμβο. Δηλαδή κάθε
                // (source, destination) ακμή έχει και μία (destination, source).
                return Arrays.asList(new Tuple2<Long, Long>(start, end),
                        new Tuple2<Long, Long>(end, start)).iterator();
            }
        });

    /*Δημιουργία νέου pairRDD που θα φτιάχνει τριάδες.
	Για κάθε κόμβο βρίσκουμε  όλους τους πιθανούς «προορισμούς»
	που μπορούν να δημιουργούν τριάδες με την JavaPairRDD.groupByKey()*/
        JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey();

	/*Δημιουργία όλων των πιθανών τριάδων με την JavaPairRDD.flatMapToPair()
	για να φτιάξουμε τα κατάλληλα key-value ζευγάρια
	όπου το key είναι ένας κόμβος που αναπαρίσταται με Tuple2<node1,node2>*/
        JavaPairRDD<Tuple2<Long,Long>, Long> possibleTriads =
                triads.flatMapToPair(new PairFlatMapFunction<
                        Tuple2<Long, Iterable<Long>>, // είσοδος
                        Tuple2<Long,Long>,            // key (έξοδος)
                        Long                         // value (έξοδος)
                        >() {
                    public Iterator<Tuple2<Tuple2<Long,Long>, Long>> call(Tuple2<Long, Iterable<Long>> s) {

                        // s._1 = Long (as a key)
                        // s._2 = Iterable<Long> (as a values)
                        Iterable<Long> values = s._2;
                        // υποθέτουμε ότι κανένας κόμβος δεν έχει την τιμή 0
                        List<Tuple2<Tuple2<Long,Long>, Long>> result = new ArrayList<Tuple2<Tuple2<Long,Long>, Long>>();

                        // Δημιουργία πιθανών τριάδων
                        for (Long value : values) {
                            Tuple2<Long,Long> k2 = new Tuple2<Long,Long>(s._1, value);
                            Tuple2<Tuple2<Long,Long>, Long> k2v2 = new Tuple2<Tuple2<Long,Long>, Long>(k2, 0l);
                            result.add(k2v2);
                        }

                        // Τα RDD's values είναι αμετάβλητα γι αυτό πρέπει να αντιγρα΄ψουμε τις τιμές
                        // με το valuesCopy
                        List<Long> valuesCopy = new ArrayList<Long>();
                        for (Long item : values) {
                            valuesCopy.add(item);
                        }
                        Collections.sort(valuesCopy);

                        // Δημιουργία πιθανών τριάδων
                        for (int i=0; i< valuesCopy.size() -1; ++i) {
                            for (int j=i+1; j< valuesCopy.size(); ++j) {
                                Tuple2<Long,Long> k2 = new Tuple2<Long,Long>(valuesCopy.get(i), valuesCopy.get(j));
                                Tuple2<Tuple2<Long,Long>, Long> k2v2 = new Tuple2<Tuple2<Long,Long>, Long>(k2, s._1);
                                result.add(k2v2);
                            }
                        }

                        return result.iterator();
                    }
                });

        JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> triadsGrouped = possibleTriads.groupByKey();

        //Δημιουργία τριγώνων
        JavaRDD<Tuple3<Long,Long,Long>> trianglesWithDuplicates =
                triadsGrouped.flatMap(new FlatMapFunction<
                        Tuple2<Tuple2<Long,Long>, Iterable<Long>>,  // είσοδος
                        Tuple3<Long,Long,Long>                      // έξοδος
                        >() {
                    @Override
                    public Iterator<Tuple3<Long,Long,Long>> call(Tuple2<Tuple2<Long,Long>, Iterable<Long>> s) {

                        // s._1 = Tuple2<Long,Long> (key) = "<nodeA><,><nodeB>"
                        // s._2 = Iterable<Long> (values) = {0, n1, n2, n3, ...} or {n1, n2, n3, ...}
                        // Ο κόμβος 0 δεν υπάρχει, είναι εικονικός
                        Tuple2<Long,Long> key = s._1;
                        Iterable<Long> values = s._2;

                        List<Long> list = new ArrayList<Long>();
                        boolean haveSeenSpecialNodeZero = false;
                        for (Long node : values) {
                            if (node == 0) {
                                haveSeenSpecialNodeZero = true;
                            }
                            else {
                                list.add(node);
                            }
                        }

                        List<Tuple3<Long,Long,Long>> result = new ArrayList<Tuple3<Long,Long,Long>>();
                        if (haveSeenSpecialNodeZero) {
                            if (list.isEmpty()) {
                                // εάν δεν βερθούν τριάδες γυρνάει κενό;
                                return result.iterator();
                            }
                            for (long node : list) {
                                long[] aTriangle = {key._1, key._2, node};
                                Arrays.sort(aTriangle);
                                Tuple3<Long,Long,Long> t3 = new Tuple3<Long,Long,Long>(aTriangle[0],
                                        aTriangle[1],
                                        aTriangle[2]);
                                result.add(t3);
                            }
                        }
                        else {

                            return result.iterator();
                        }

                        return result.iterator();
                    }
                });

        // Απαλοιφή των διπλότυπων τριγώνων
        JavaRDD<Tuple3<Long,Long,Long>> uniqueTriangles = trianglesWithDuplicates.distinct();

        //Υπολογισμός Clustering Coefficient
        long numberOfTriangles = uniqueTriangles.count();
        long numberOfTriads = triadsGrouped.count();
        long ClusteringCoefficient = (3*numberOfTriangles)/numberOfTriads;
        System.out.println("Global Clustering Coefficient: "+ClusteringCoefficient);
        sc.close();
        System.exit(0);
    }

}