package ua.edu.nubip.sparkdatastarter.unsafe.collection;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class OrderedBag<T> {

    private List<T> list = new ArrayList<>();

    public OrderedBag(T[] args) {
        this.list = new ArrayList<T>(asList(args));
    }

    public T takeAndRemove() {
        return list.remove(0);
    }

    public int size() {
        return list.size();
    }

}
