Feature: Run a step

  Scenario: simple run
    Given a simple step exists
    When simple step runs
    Then it consumes and processes every message

  Scenario: use lifecycle methods
    Given step has custom lifecycle
    When simple step runs with lifecycle
    Then it consumes and processes every message
    And all lifecycle methods were executed
