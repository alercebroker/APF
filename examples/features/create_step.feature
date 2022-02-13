Feature: Create a step
  APF has a command line interface allowing to create steps
  There are 3 types of steps: simple, component and composite
  Creating a step with the CLI will provide all the
  required boilerplate for writing a step

  @fixture.tmpdir
  Scenario: Create a simple step
    When user calls the new-step command
    Then a new directory is created with the step boilerplate

  @fixture.tmpdir
  Scenario: Create a component step
    When user calls the new-step command with "component" argument
    Then a new directory is created with the step boilerplate
    And the config has type "component"

  @fixture.tmpdir
  Scenario: Create a composite step
    When user calls the new-step command with "composite" argument
    Then a new directory is created with the step boilerplate
    And the config has type "composite"
