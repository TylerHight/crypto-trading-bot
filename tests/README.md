# Test Suites

`tests/` contains repository-level verification organized by the kind of guarantee it provides. Tests may span deployment units, contracts, or recorded datasets; narrow tests that are tightly coupled to one package may also live beside that package if the project tooling supports it.

The suite must remain runnable locally and in CI without production credentials. External APIs use recorded fixtures, fakes, or explicitly isolated test environments. Never allow automated tests to submit production orders.
