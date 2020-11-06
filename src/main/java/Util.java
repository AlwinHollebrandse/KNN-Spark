package com.alwin;

import org.apache.spark.mllib.regression.LabeledPoint;


public class Util {
    public static double calcDistance(LabeledPoint trainInstance, LabeledPoint testInstance) {
        // calc distance
        double distance = 0;
        // for (int i = 0; i < trainInstance.getFeatures().size() - 1; i++) {
        //     double diff = Double.parseDouble(testInstance.getFeatures().apply(i)) - Double.parseDouble(trainInstance.getFeatures().apply(i));
        //     distance += diff * diff;
        // }

        // distance = Math.sqrt(distance);
        return distance;
    }
}
