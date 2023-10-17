/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.cdk.integrations.base.DestinationConfig;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeBulkLoadDestinationTest {

  @BeforeEach
  public void setup() {
    DestinationConfig.initialize(Jsons.emptyObject());
  }

  @Test
  void testGetSpec() throws Exception {
    System.out.println(new SnowflakeBulkLoadDestination().spec().getConnectionSpecification());
    assertEquals(Jsons.deserialize(MoreResources.readResource("expected_spec.json"), ConnectorSpecification.class),
        new SnowflakeBulkLoadDestination().spec());
  }

}