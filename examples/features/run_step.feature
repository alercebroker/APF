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


  Scenario: composite step run
    Given composite_example01 step
    When step runs
    Then it consumes and processes every message

  Scenario: composite step run with multiple internal pipelines
    Given composite_example02 step
    When step runs
    Then it consumes and processes every message
