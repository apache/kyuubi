export const MENUS = [
  {
    label: 'Overview',
    icon: 'Odometer',
    router: '/overview',
  },
  {
    label: 'Workload',
    icon: 'List',
    children: [
      {
        label: 'Analysis',
        icon: 'VideoPlay',
        router: '/workload/analysis',
      },
      {
        label: 'Queue',
        icon: 'Select',
        router: '/workload/queue',
      },
      {
        label: 'Session',
        icon: 'Select',
        router: '/workload/session',
      },
      {
        label: 'Query',
        icon: 'Select',
        router: '/workload/query',
      },
    ],
  },
  {
    label: 'Operation',
    icon: 'List',
    children: [
      {
        label: 'Running Jobs',
        icon: 'VideoPlay',
        router: '/operation/runningJobs',
      },
      {
        label: 'Completed Jobs',
        icon: 'Select',
        router: '/operation/completedJobs',
      },
    ],
  },
  {
    label: 'Contact Us',
    icon: 'PhoneFilled',
    router: '/contact',
  },
]
