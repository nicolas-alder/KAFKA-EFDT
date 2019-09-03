package java.com.hpi.msd;

import com.google.common.collect.ListMultimap;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;

public class TreeworkerProcessorTest {

    @Mock
    KeyValueStore<String, ListMultimap> kvStore;


    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void test() {
        assertEquals(2, 2);
    }
}
