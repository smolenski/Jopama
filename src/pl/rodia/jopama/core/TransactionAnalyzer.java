package pl.rodia.jopama.core;

import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.UnifiedAction;

public interface TransactionAnalyzer {

	UnifiedAction getChange(ObjectId transactionId);

}
