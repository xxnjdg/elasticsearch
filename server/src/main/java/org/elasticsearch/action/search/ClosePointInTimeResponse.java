/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ClosePointInTimeResponse extends ClearScrollResponse {
    public ClosePointInTimeResponse(boolean succeeded, int numFreed) {
        super(succeeded, numFreed);
    }

    public ClosePointInTimeResponse(StreamInput in) throws IOException {
        super(in);
    }
}
