import openmct from 'openmct';

openmct.install(openmct.plugins.LocalStorage());
openmct.install(openmct.plugins.MyItems());
openmct.install(openmct.plugins.UTCTimeSystem());
openmct.install(openmct.plugins.Clock({ enableClockIndicator: true }));
openmct.install(openmct.plugins.Espresso());

openmct.time.setTimeSystem('utc', {
  start: Date.now() - 15 * 60 * 1000,
  end: Date.now(),
});

openmct.start();
