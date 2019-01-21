import org.apache.log4j.Logger;

public class LogTest {

	static Logger log = Logger.getLogger(LogTest.class);

	public static void main(String[] args) {
		log.trace("!! TRACE !!");
		log.debug("!! DEBUG !!");
		log.info("!! INFO !!");
		log.warn("!! WARN !!");
		log.error("!! ERROR !!");
		log.fatal("!! FATAL !!");
	}


}
