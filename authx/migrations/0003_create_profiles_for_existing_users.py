# Generated manually for creating UserProfile records for existing users

from django.db import migrations
from django.contrib.auth import get_user_model


def create_user_profiles(apps, schema_editor):
    """
    Create UserProfile records for all existing users with default values
    """
    User = apps.get_model('auth', 'User')
    UserProfile = apps.get_model('authx', 'UserProfile')

    # Get all users
    all_users = User.objects.all()

    profiles_to_create = []
    for user in all_users:
        # Check if profile already exists to avoid duplicates
        if not UserProfile.objects.filter(user_id=user.id).exists():
            profiles_to_create.append(
                UserProfile(
                    user=user,
                    language='EN',
                    unit_system='Imperial',
                    preferred_mode='Light'
                )
            )

    # Bulk create all profiles
    if profiles_to_create:
        UserProfile.objects.bulk_create(profiles_to_create)
        print(f"Created {len(profiles_to_create)} UserProfile records for existing users")


def reverse_create_user_profiles(apps, schema_editor):
    """
    Remove all UserProfile records (reverse migration)
    """
    UserProfile = apps.get_model('authx', 'UserProfile')
    UserProfile.objects.all().delete()
    print("Deleted all UserProfile records")


class Migration(migrations.Migration):

    dependencies = [
        ('authx', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(
            create_user_profiles,
            reverse_create_user_profiles,
        ),
    ]
