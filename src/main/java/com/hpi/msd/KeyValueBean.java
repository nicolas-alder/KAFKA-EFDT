package com.hpi.msd;

import com.google.common.collect.ListMultimap;

import java.util.Objects;

/**
 * A simple bean used by {@link Query} when responding to
 * {@link Query#byKey(String, String)}.
 *
 * We use this JavaBean based approach as it fits nicely with JSON serialization provided by
 * jax-rs/jersey
 */
public class KeyValueBean {

    private String key;
    private ListMultimap value;

    public KeyValueBean() {}

    public KeyValueBean(final String key, final ListMultimap value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() { return key; }

    public void setKey(final String key) {
        this.key = key;
    }

    public ListMultimap getValue() {
        return value;
    }

    public void setValue(final ListMultimap value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyValueBean that = (KeyValueBean) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValueBean{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
