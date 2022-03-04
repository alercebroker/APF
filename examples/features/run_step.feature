Feature: Run a step

  Scenario: simple run
    Given simple_example01 step
    When step runs
    Then it consumes and processes every message

  Scenario: use lifecycle methods
    Given lifecycle_example01 step
    When step runs
    Then it consumes and processes every message
    And all lifecycle methods were executed

  Scenario: composite step with multiple internal pipelines that are joined
    Given composite_example01/composite step
    And composite_example01/data directory has no output
    When step runs
    Then it consumes and processes every message
    And it produces joined output in composite_example01/data
    Then composite_example01/data directory has no output
