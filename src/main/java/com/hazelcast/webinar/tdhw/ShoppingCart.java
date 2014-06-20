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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ShoppingCart implements Serializable {
    private List<ShoppingCartItem> items = new ArrayList<>();
    public long total = 0;

    public void addItem(ShoppingCartItem item) {
        items.add(item);
        total += item.cost * item.quantity;
    }

    public void removeItem(int index) {
        ShoppingCartItem item = items.remove(index);
        total -= item.cost * item.quantity;
    }

    public int size() {
        return items.size();
    }

    public List<ShoppingCartItem> getItems() {
        return items;
    }
}