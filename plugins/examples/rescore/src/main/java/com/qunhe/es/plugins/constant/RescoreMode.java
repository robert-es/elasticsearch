/*
 * RescoreMode.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.constant;

import java.util.Locale;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/22
 */
public enum RescoreMode {
    Avg {
        @Override
        public float combine(float primary, float secondary) {
            return (primary + secondary) / 2;
        }

        @Override
        public String toString() {
            return "avg";
        }
    },
    Max {
        @Override
        public float combine(float primary, float secondary) {
            return Math.max(primary, secondary);
        }

        @Override
        public String toString() {
            return "max";
        }
    },
    Min {
        @Override
        public float combine(float primary, float secondary) {
            return Math.min(primary, secondary);
        }

        @Override
        public String toString() {
            return "min";
        }
    },
    Total {
        @Override
        public float combine(float primary, float secondary) {
            return primary + secondary;
        }

        @Override
        public String toString() {
            return "sum";
        }
    },
    Multiply {
        @Override
        public float combine(float primary, float secondary) {
            return primary * secondary;
        }

        @Override
        public String toString() {
            return "product";
        }
    };

    public abstract float combine(float primary, float secondary);


    public static RescoreMode fromString(String scoreMode) {
        for (RescoreMode mode : values()) {
            if (scoreMode.toLowerCase(Locale.ROOT).equals(mode.name().toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }

        throw new IllegalArgumentException("illegal score_mode [" + scoreMode + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}