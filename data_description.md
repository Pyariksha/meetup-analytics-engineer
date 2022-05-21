# Data Description

For the assignment the following files will be provided:

- `groups.json`: An array containing all groups, including metadata and topics that apply to a group.
- `users.json`:  An array containing all users in the database, including metadata. All group memberships are contained in the user object.
- `events.json`: An array containing all meetup events, including metadata and RSVPs.
- `venues.json`: An array containing details about all venues referenced from the events data.

Below you can find an outline of the contents of each file. 

## Groups

Each group object has the following fields:

- `city`: Name of the city where the group resides.
- `created`: Timestamp of when the group was created.
- `description`: Description of the group.
- `name`: Name of the group.
- `lat`: Latitude of the place where the group resides.
- `lon`: Longitude of the place where the group resides.
- `link`: Link to the group's homepage.
- `group_id`: Unique identifier for this group. This is used as a reference in other objects.
- `topics`: Array of topics that this group discusses or otherwise associates with.

## Users

Each user object has the following fields:

- `user_id`: A unique identifier for this user.
Note that this identifier has no relation to internal identifiers from meetup.com; users can not be associated with actual accounts using this dataset.
Such is for privacy reasons.
- `city`: City where the user resides.
- `country`: Country where the user resides.
- `hometown`: Town that the user specified as their home town.
- `memberships`: Array of membership objects, containing the following fields:
    - `joined`: Timestamp of the moment the user joined this group.
    - `group_id`: The unique identifier of the group that the user has joined.

## Events

Each event object has the following fields:

- `group_id`: The unique identifier of the group that organised this event.
- `name`: The title of the event.
- `description`: The description of the event.
- `created`: Timestamp of the moment the event was created by the organiser.
- `time`: The timestamp of when the event will start (or has started).
- `duration`: The duration of the event, in seconds.
- `rsvp_limit`: The maximum number of YES RSVPs that this event will allow.
- `venue_id`: Unique identifier of the venue where this event takes place (see below).
- `status`: The status of the event. Possible values include 'past' and 'upcoming', meaning the the event has already taken place or that the event is planned for the future respectively.
Keep in mind that this dataset was created at some point in the past, so an event marked as upcoming might not actually have a time in the past given the time of creating the solution.
- `rsvps`: An array of RSVP objects, which contain the following fields:
    - `user_id`: The unique identifier of the user that RSVPed for this event.
    - `when`: Timestamp of the moment the user gave their RSVP.
    - `response`: Yes or No, the indication of whether this user will attend the event.
    - `guests`: If permitted, the number of guests that the user is planning to bring to the event.

## Venues

Each venue has the following fields:

- `venue_id`: A unique identifier for this venue.
- `name`: The name of the venue.
- `city`: The city where the venue is located.
- `country`: The country where the venue is located.
- `lat`: The lattitude of the venue location.
- `lon`: The longitude of the vanue location.