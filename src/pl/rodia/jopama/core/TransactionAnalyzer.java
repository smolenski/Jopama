package pl.rodia.jopama.core;

import pl.rodia.jopama.data.UnifiedAction;

public interface TransactionAnalyzer {

	UnifiedAction getChange(Integer transactionId);

}
