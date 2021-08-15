package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import java.util.List;
import java.util.Random;

public interface ChooseRouteStrategy {
    RouteMeta choose(List<RouteMeta> routeMetas);
    public static ChooseRouteStrategy RANDOM = new RandomChooseRouteStrategy();
    class HashCodeChooseRouteStrategy implements ChooseRouteStrategy {
        int hashCode;
        public HashCodeChooseRouteStrategy(int code) {
            this.hashCode = code;
        }
        @Override
        public RouteMeta choose(List<RouteMeta> routeMetas) {
            if(routeMetas.isEmpty()) {
                return null;
            }
            return routeMetas.get(hashCode % routeMetas.size());
        }
    }

    class RandomChooseRouteStrategy implements  ChooseRouteStrategy {
        @Override
        public RouteMeta choose(List<RouteMeta> routeMetas) {
            if(routeMetas.isEmpty()) {
                return null;
            }
            int random = new Random().nextInt(routeMetas.size());
            return routeMetas.get(random);
        }
    }
}



