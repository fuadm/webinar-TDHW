/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.webinar.tdhw;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.*;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AmazonExample {
    private static final String ORDERS = "orders";
    final List<HazelcastInstance> instances;
    final static Random random = new Random(System.currentTimeMillis());
    final String[] keys = PerformanceTester.generateStringArray(1000);
    final static ShoppingCartItemFactory shoppingCartItemFactory = new ShoppingCartItemFactory();

    public AmazonExample() {
        instances = new ArrayList<>();
        Config config = new Config();
        config.getMapConfig(ORDERS).setInMemoryFormat(InMemoryFormat.BINARY);
        for (int i = 0; i < 4; i++) {
            HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
            instances.add(hazelcastInstance);
        }
//        map = hazelcastInstance.getMap("orders");
    }

    public static void main(String[] input) {
        AmazonExample test = new AmazonExample();
        test.test();
    }

    private void test() {
        PerformanceTester test = new PerformanceTester(instances.get(0), 10);
        test.start(doPutWithoutLock());
//        test.start(doPutWithLock());
//        test.start(doPutInTwoPhaseTransaction());
//        test.start(doPutInLocalTransaction());
//        test.start(doPutWithCAS());
//        test.start(doPutInExecutorServiceWithLock());
//        test.start(doPutWithEntryProcessor());
//        test.start(doPutWithMultiMap());
    }

    public HazelcastInstance getInstance() {
        return instances.get(random.nextInt(instances.size()));
    }

    public Runnable doPutInLocalTransaction() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.LOCAL);
                TransactionContext context = getInstance().newTransactionContext(options);
                context.beginTransaction();
                try {
                    TransactionalMap<String, ShoppingCart> map = context.getMap(ORDERS);
                    ShoppingCart cart = map.getForUpdate(key);
                    if (cart == null) cart = new ShoppingCart();
                    if (cart.total < 1000) {
                        cart.addItem(shoppingCartItemFactory.createAnItem());
                    } else
                        cart.removeItem(random.nextInt(cart.size()));
                    map.put(key, cart);
                    context.commitTransaction();
                } catch (Exception e) {
                    context.rollbackTransaction();
                }
            }
        };
    }

    public Runnable doPutInExecutorServiceWithLock() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                IExecutorService ex = getInstance().getExecutorService("order-processing");
                ex.executeOnKeyOwner(new PutWithLock(key), key);
            }
        };
    }

    public static class PutWithLock implements Runnable, Serializable, HazelcastInstanceAware {
        final String key;

        public PutWithLock(String key) {
            this.key = key;
        }

        @Override
        public void run() {
            IMap<String, ShoppingCart> map = instance.getMap(ORDERS);
            map.lock(key);
            try {
                ShoppingCart cart = map.get(key);
                if (cart == null) cart = new ShoppingCart();
                if (cart.total < 1000) {
                    cart.addItem(shoppingCartItemFactory.createAnItem());
                } else
                    cart.removeItem(random.nextInt(cart.size()));
                map.put(key, cart);
            } finally {
                map.unlock(key);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }

        HazelcastInstance instance;
    }

    public Runnable doPutInTwoPhaseTransaction() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
                TransactionContext context = getInstance().newTransactionContext(options);
                context.beginTransaction();
                try {
                    TransactionalMap<String, ShoppingCart> map = context.getMap(ORDERS);
                    ShoppingCart cart = map.getForUpdate(key);
                    if (cart == null) cart = new ShoppingCart();
                    if (cart.total < 1000) {
                        cart.addItem(shoppingCartItemFactory.createAnItem());
                    } else
                        cart.removeItem(random.nextInt(cart.size()));
                    map.put(key, cart);
                    context.commitTransaction();
                } catch (Exception e) {
                    context.rollbackTransaction();
                }
            }
        };
    }

    public Runnable doPutWithLock() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                IMap<String, ShoppingCart> map = getInstance().getMap(ORDERS);
                map.lock(key);
                try {
                    ShoppingCart cart = map.get(key);
                    if (cart == null) cart = new ShoppingCart();
                    if (cart.total < 1000) {
                        cart.addItem(shoppingCartItemFactory.createAnItem());
                    } else
                        cart.removeItem(random.nextInt(cart.size()));
                    map.put(key, cart);
                } finally {
                    map.unlock(key);
                }
            }
        };
    }

    public Runnable doPutWithoutLock() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                IMap<String, ShoppingCart> map = getInstance().getMap(ORDERS);
                ShoppingCart cart = map.get(key);
                if (cart == null) cart = new ShoppingCart();
                if (cart.total < 1000) {
                    cart.addItem(shoppingCartItemFactory.createAnItem());
                } else
                    cart.removeItem(random.nextInt(cart.size()));
                map.put(key, cart);
            }
        };
    }

    public Runnable doPutWithCAS() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                IMap<String, ShoppingCart> map = getInstance().getMap(ORDERS);
                boolean done = false;
                while (!done) {
                    ShoppingCart oldCart = map.get(key);
                    final ShoppingCart newCart = new ShoppingCart();
                    if (oldCart != null) {
                        oldCart.getItems().forEach((item) -> newCart.addItem(item));
                    }
                    if (newCart.total < 1000) {
                        newCart.addItem(shoppingCartItemFactory.createAnItem());
                    } else {
                        newCart.removeItem(random.nextInt(newCart.size()));
                    }
                    if (oldCart == null) {
                        done = (map.putIfAbsent(key, newCart) == null);
                    } else {
                        done = map.replace(key, oldCart, newCart);
                    }
                }
            }
        };
    }

    public Runnable doPutWithEntryProcessor() {
        return new Runnable() {
            @Override
            public void run() {
                String key = keys[random.nextInt(keys.length)];
                IMap<String, ShoppingCart> map = getInstance().getMap(ORDERS);
                map.executeOnKey(key, new UpdateEntryProcessor());
            }
        };
    }

    public static class UpdateEntryProcessor implements EntryProcessor<String, ShoppingCart>, Serializable {

        @Override
        public Object process(Map.Entry<String, ShoppingCart> entry) {
            ShoppingCart cart = entry.getValue();
            if (cart == null) cart = new ShoppingCart();
            if (cart.total < 1000) {
                cart.addItem(shoppingCartItemFactory.createAnItem());
            } else
                cart.removeItem(random.nextInt(cart.size()));
            entry.setValue(cart);
            return null;
        }

        @Override
        public EntryBackupProcessor<String, ShoppingCart> getBackupProcessor() {
            return new EntryBackupProcessor<String, ShoppingCart>() {
                @Override
                public void processBackup(Map.Entry entry) {
                    process(entry);
                }
            };
        }
    }
}
