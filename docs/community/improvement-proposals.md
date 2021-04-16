# Kyuubi Project Improvement Proposals (KBIP)

The purpose of an KBIP is to inform and involve the user community in major improvements to the Kyuubi codebase throughout the development process, to increase the likelihood that user needs are met.

KBIPs should be used for significant user-facing or cross-cutting changes, not small incremental improvements. When in doubt, if a committer thinks a change needs an KBIP, it does.

## What is a KBIP?

An KBIP is similar to a product requirement document commonly used in product management.

An KBIP:

- Is a ticket labeled “KBIP” proposing a major improvement or change to Kyuubi
- Follows the template defined below
- Includes discussions on the ticket about the proposal

## Who?

Any **community member** can help by discussing whether an KBIP is likely to meet their needs, and by proposing KBIPs.

**Contributors** can help by discussing whether an KBIP is likely to be technically feasible.

**Committers** can help by discussing whether an KBIP aligns with long-term project goals, and by shepherding KBIPs.

**KBIP Author** is any community member who authors a KBIP and is committed to pushing the change through the entire process. KBIP authorship can be transferred.

**KBIP Shepherd** is a PMC member who is committed to shepherding the proposed change throughout the entire process. Although the shepherd can delegate or work with other committers in the development process, the shepherd is ultimately responsible for the success or failure of the KBIP. Responsibilities of the shepherd include, but are not limited to:

- Be the advocate for the proposed change
- Help push forward on design and achieve consensus among key stakeholders
- Review code changes, making sure the change follows project standards
- Get feedback from users and iterate on the design & implementation
- Uphold the quality of the changes, including verifying whether the changes satisfy the goal of the KBIP and are absent of critical bugs before releasing them

## KBIP Process
### Proposing an KBIP

Anyone may propose an KBIP, using the document template below. Please only submit an KBIP if you are willing to help, at least with discussion.

If an KBIP is too small or incremental and should have been done through the normal JIRA process, a committer should remove the KBIP label.


### KBIP Document Template

A KBIP document is a short document with a few questions, inspired by the Heilmeier Catechism:

- Q1. What are you trying to do? Articulate your objectives using absolutely no jargon.

- Q2. What problem is this proposal NOT designed to solve?

- Q3. How is it done today, and what are the limits of current practice?

- Q4. What is new in your approach and why do you think it will be successful?

- Q5. Who cares? If you are successful, what difference will it make?

- Q6. What are the risks?

- Q7. How long will it take?

- Q8. What are the mid-term and final “exams” to check for success?

- Appendix A. Proposed API Changes. Optional section defining APIs changes, if any. Backward and forward compatibility must be taken into account.

- Appendix B. Optional Design Sketch: How are the goals going to be accomplished? Give sufficient technical detail to allow a contributor to judge whether it's likely to be feasible. Note that this is not a full design document.

- Appendix C. Optional Rejected Designs: What alternatives were considered? Why were they rejected? If no alternatives have been considered, the problem needs more thought.

### Discussing an KBIP

All discussion of an KBIP should take place in a public forum, preferably the discussion attached to the ticket. Any discussions that happen offline should be made available online for the public via meeting notes summarizing the discussions.

### Implementing an KBIP

Implementation should take place via the [contribution guidelines](./contributions.md). Changes that require KBIPs typically also require design documents to be written and reviewed.
