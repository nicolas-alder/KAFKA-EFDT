package com.hpi.msd;


public class ThresholdParameter {

    double threshold = 0.95;

    private static ThresholdParameter instance;

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public double getThreshold() {
        return threshold;
    }

    public static ThresholdParameter getInstance() {
            if (ThresholdParameter.instance == null) {
                ThresholdParameter.instance = new ThresholdParameter();
            }
            return ThresholdParameter.instance;
        }

    }


