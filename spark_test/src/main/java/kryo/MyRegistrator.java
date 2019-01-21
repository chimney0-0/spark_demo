package kryo;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator{
    /* (non-Javadoc)
     * @see org.apache.spark.serializer.KryoRegistrator#registerClasses(com.esotericsoftware.kryo.Kryo)
     */
    public void registerClasses(Kryo arg0) {
        // TODO Auto-generated method stub
        arg0.register(Qualify.class);
//        arg0.register(String.class);
//        arg0.register(Integer.class);
    }
}
